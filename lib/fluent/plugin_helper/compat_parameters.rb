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

require 'fluent/config/element'

module Fluent
  module PluginHelper
    module CompatParameters
      # This plugin helper is to bring old-fashioned buffer/other
      # configuration parameters to v0.14 plugin API configurations.
      # This helper is mainly to convert plugins from v0.12 API
      # to v0.14 API safely, without breaking user deployment.

      PARAMS_MAP = {
        "buffer_type" => "@type",
        "buffer_path" => "path",
        "num_threads"                 => "flush_thread_count",
        "flush_interval"              => "flush_interval",
        "try_flush_interval"          => "flush_thread_interval",
        "queued_chunk_flush_interval" => "flush_thread_burst_interval",
        "disable_retry_limit" => "retry_forever",
        "retry_limit"         => "retry_max_times",
        "max_retry_wait"      => "retry_max_interval",
        "buffer_chunk_limit"  => "chunk_limit_size",
        "buffer_queue_limit"  => "queue_length_limit",
        "buffer_queue_full_action" => "overflow_action",
        "flush_at_shutdown" => "flush_at_shutdown",
      }

      TIME_SLICED_PARAMS = {
        "time_slice_format" => nil,
        "time_slice_wait" => "timekey_wait",
      }

      def compat_parameters_default_chunk_key
        # '', 'time' or 'tag'
        raise NotImplementedError, "return one of '', 'time' or 'tag'"
      end

      def configure(conf)
        if conf.elements('buffer').empty? && PARAMS_MAP.keys.any?{|k| conf.has_key?(k) } || TIME_SLICED_PARAMS.keys.any?{|k| conf.has_key?(k) }
          # TODO: warn obsolete parameters if these are deprecated
          attr = {}
          PARAMS_MAP.each do |compat, current|
            next unless current
            if conf.has_key?(compat)
              if compat == 'buffer_queue_full_action' && conf[compat] == 'exception'
                attr[current] = 'throw_exception'
              else
                attr[current] = conf[compat]
              end
            end
          end
          TIME_SLICED_PARAMS.each do |compat, current|
            next unless current
            attr[current] = conf[compat] if conf.has_key?(compat)
          end

          chunk_key = nil

          if conf.has_key?('time_slice_format')
            chunk_key = 'time'
            attr['timekey'] = case conf['time_slice_format']
                              when /\%S/ then 1
                              when /\%M/ then 60
                              when /\%H/ then 3600
                              when /\%d/ then 86400
                              else
                                raise Fluent::ConfigError, "time_slice_format only with %Y or %m is too long"
                              end
          else
            chunk_key = compat_parameters_default_chunk_key
            if chunk_key == 'time'
              attr['timekey'] = 86400 # TimeSliceOutput.time_slice_format default value is '%Y%m%d'
            end
          end

          e = Fluent::Config::Element.new('buffer', chunk_key, attr, [])
          conf.elements << e
        end

        super
      end
    end
  end
end
