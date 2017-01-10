#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/event'
require 'fluent/time'
require 'fluent/configurable'
require 'socket'

module Fluent
  module PluginHelper
    module Inject
      def inject_values_to_record(tag, time, record)
        return record unless @_inject_enabled

        r = record.dup
        if @_inject_hostname_key
          r[@_inject_hostname_key] = @_inject_hostname
        end
        if @_inject_worker_id_key
          r[@_inject_worker_id_key] = @_inject_worker_id
        end
        if @_inject_tag_key
          r[@_inject_tag_key] = tag
        end
        if @_inject_time_key
          r[@_inject_time_key] = @_inject_time_formatter.call(time)
        end

        r
      end

      def inject_values_to_event_stream(tag, es)
        return es unless @_inject_enabled

        new_es = Fluent::MultiEventStream.new
        es.each do |time, record|
          r = record.dup
          if @_inject_hostname_key
            r[@_inject_hostname_key] = @_inject_hostname
          end
          if @_inject_worker_id_key
            r[@_inject_worker_id_key] = @_inject_worker_id
          end
          if @_inject_tag_key
            r[@_inject_tag_key] = tag
          end
          if @_inject_time_key
            r[@_inject_time_key] = @_inject_time_formatter.call(time)
          end
          new_es.add(time, r)
        end

        new_es
      end

      module InjectParams
        include Fluent::Configurable
        config_section :inject, required: false, multi: false, param_name: :inject_config do
          config_param :hostname_key, :string, default: nil
          config_param :hostname, :string, default: nil
          config_param :worker_id_key, :string, default: nil
          config_param :tag_key, :string, default: nil
          config_param :time_key, :string, default: nil

          # To avoid defining :time_type twice
          config_param :time_type, :enum, list: [:float, :unixtime, :string], default: :float

          Fluent::TimeMixin::TIME_PARAMETERS.each do |name, type, opts|
            config_param name, type, opts
          end
        end
      end

      def self.included(mod)
        mod.include InjectParams
      end

      def initialize
        super
        @_inject_enabled = false
        @_inject_hostname_key = nil
        @_inject_hostname = nil
        @_inject_worker_id_key = nil
        @_inject_worker_id = nil
        @_inject_tag_key = nil
        @_inject_time_key = nil
        @_inject_time_formatter = nil
      end

      def configure(conf)
        super

        if @inject_config
          @_inject_hostname_key = @inject_config.hostname_key
          if @_inject_hostname_key
            if self.respond_to?(:buffer_config)
              # Output plugin cannot use "hostname"(specified by @hostname_key),
              # injected by this plugin helper, in chunk keys.
              # This plugin helper works in `#format` (in many cases), but modified record
              # don't have any side effect in chunking of output plugin.
              if self.buffer_config.chunk_keys.include?(@_inject_hostname_key)
                log.error "Use filters to inject hostname to use it in buffer chunking."
                raise Fluent::ConfigError, "the key specified by 'hostname_key' in <inject> cannot be used in buffering chunk key."
              end
            end

            @_inject_hostname =  @inject_config.hostname
            unless @_inject_hostname
              @_inject_hostname = ::Socket.gethostname
              log.info "using hostname for specified field", host_key: @_inject_hostname_key, host_name: @_inject_hostname
            end
          end
          @_inject_worker_id_key = @inject_config.worker_id_key
          if @_inject_worker_id_key
            @_inject_worker_id = fluentd_worker_id # get id here, because #with_worker_config method may be used only for #configure in tests
          end
          @_inject_tag_key = @inject_config.tag_key
          @_inject_time_key = @inject_config.time_key
          if @_inject_time_key
            @_inject_time_formatter = case @inject_config.time_type
                                      when :float then ->(time){ time.to_r.truncate(+6).to_f } # microsecond floating point value
                                      when :unixtime then ->(time){ time.to_i }
                                      else
                                        localtime = @inject_config.localtime && !@inject_config.utc
                                        Fluent::TimeFormatter.new(@inject_config.time_format, localtime, @inject_config.timezone)
                                      end
          else
            if @inject_config.time_format
              log.warn "'time_format' specified without 'time_key', will be ignored"
            end
          end

          @_inject_enabled = @_inject_hostname_key || @_inject_worker_id_key || @_inject_tag_key || @_inject_time_key
        end
      end
    end
  end
end
