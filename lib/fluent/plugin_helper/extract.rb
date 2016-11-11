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

module Fluent
  module PluginHelper
    module Extract
      def extract_tag_from_record(record)
        return nil unless @_extract_enabled

        if @_extract_tag_key && record.has_key?(@_extract_tag_key)
          v = @_extract_keep_tag_key ? record[@_extract_tag_key] : record.delete(@_extract_tag_key)
          return v.to_s
        end

        nil
      end

      def extract_time_from_record(record)
        return nil unless @_extract_enabled

        if @_extract_time_key && record.has_key?(@_extract_time_key)
          v = @_extract_keep_time_key ? record[@_extract_time_key] : record.delete(@_extract_time_key)
          return @_extract_time_parser.call(v)
        end

        nil
      end

      module ExtractParams
        include Fluent::Configurable
        config_section :extract, required: false, multi: false, param_name: :extract_config do
          config_param :tag_key, :string, default: nil
          config_param :keep_tag_key, :bool, default: false
          config_param :time_key, :string, default: nil
          config_param :keep_time_key, :bool, default: false

          # To avoid defining :time_type twice
          config_param :time_type, :enum, list: [:float, :unixtime, :string], default: :float

          Fluent::TimeMixin::TIME_PARAMETERS.each do |name, type, opts|
            config_param name, type, opts
          end
        end
      end

      def self.included(mod)
        mod.include ExtractParams
      end

      def initialize
        super
        @_extract_enabled = false
        @_extract_tag_key = nil
        @_extract_keep_tag_key = nil
        @_extract_time_key = nil
        @_extract_keep_time_key = nil
        @_extract_time_parser = nil
      end

      def configure(conf)
        super

        if @extract_config
          @_extract_tag_key = @extract_config.tag_key
          @_extract_keep_tag_key = @extract_config.keep_tag_key
          @_extract_time_key = @extract_config.time_key
          if @_extract_time_key
            @_extract_keep_time_key = @extract_config.keep_time_key
            @_extract_time_parser = case @extract_config.time_type
                                    when :float then Fluent::NumericTimeParser.new(:float)
                                    when :unixtime then Fluent::NumericTimeParser.new(:unixtime)
                                    else
                                      localtime = @extract_config.localtime && !@extract_config.utc
                                      Fluent::TimeParser.new(@extract_config.time_format, localtime, @extract_config.timezone)
                                    end
          else
            if @extract_config.time_format
              log.warn "'time_format' specified without 'time_key', will be ignored"
            end
          end

          @_extract_enabled = @_extract_tag_key || @_extract_time_key
        end
      end
    end
  end
end
