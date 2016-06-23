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

      BUFFER_PARAMS = {
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

      BUFFER_TIME_SLICED_PARAMS = {
        "time_slice_format" => nil,
        "time_slice_wait" => "timekey_wait",
      }

      PARSER_PARAMS = {
        "format" => "@type",
        "time_key"    => "time_key",
        "time_format" => "time_format",
        "delimiter"   => "delimiter",
        "json_parser"      => "json_parser", # JSONParser
        "label_delimiter"  => "label_delimiter", # LabeledTSVParser
        "format_firstline" => "format_firstline", # MultilineParser
        "message_key"      => "message_key", # NoneParser
        "with_priority"    => "with_priority", # SyslogParser
      }

      FORMATTER_PARAMS = {
        "format" => "@type",

        # TODO: convert to use 'inject'
        "include_time_key" => "include_time_key",
        "include_tag_key"  => "include_tag_key",
        "time_key"      => "time_key",
        "time_format"   => "time_format",
        "time_as_epoch" => "time_as_epoch",
        "localtime" => "localtime",
        "utc"       => "utc",
        "timezone"      => "timezone",
        "tag_key" => "tag_key",

        "delimiter" => "delimiter",

        "force_quotes" => "force_quotes", # CsvFormatter
        "fields" => "fields", # CsvFormatter
        "json_parser" => "json_parser", # JSONFormatter
        "label_delimiter" => "label_delimiter", # LabeledTSVFormatter
        "output_time" => "output_time", # OutFileFormatter
        "output_tag"  => "output_tag", # OutFileFormatter
        "message_key" => "message_key", # SingleValueFormatter
        "add_newline" => "add_newline", # SingleValueFormatter
        "output_type" => "output_type", # StdoutFormatter
      }

      def compat_parameters_buffer_default_chunk_key
        # '', 'time' or 'tag'
        raise NotImplementedError, "return one of '', 'time' or 'tag'"
      end

      def configure(conf)
        case self
        when Fluent::Plugin::Output
          compat_parameters_buffer(conf)
        when Fluent::Plugin::Parser
          compat_parameters_parser(conf)
        when Fluent::Plugin::Formatter
          compat_parameters_formatter(conf)
        end

        super
      end

      def compat_parameters_copy_to_subsection_attributes(conf, params, &block)
        attr = {}
        params.each do |compat, current|
          next unless current
          if conf.has_key?(compat)
            if block_given?
              attr[current] = block.call(compat, conf[compat])
            else
              attr[current] = conf[compat]
            end
          end
        end
        attr
      end

      def compat_parameters_buffer(conf)
        # return immediately if <buffer> section exists, or any buffer-related parameters don't exist
        return unless conf.elements('buffer').empty?
        return if (BUFFER_PARAMS.keys + BUFFER_TIME_SLICED_PARAMS.keys).all?{|k| !conf.has_key?(k) }

        # TODO: warn obsolete parameters if these are deprecated
        buffer_params = BUFFER_PARAMS.merge(BUFFER_TIME_SLICED_PARAMS)
        attr = compat_parameters_copy_to_subsection_attributes(conf, buffer_params) do |compat_key, value|
          if compat_key == 'buffer_queue_full_action' && value == 'exception'
            'throw_exception'
          else
            value
          end
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
          chunk_key = compat_parameters_buffer_default_chunk_key
          if chunk_key == 'time'
            attr['timekey'] = 86400 # TimeSliceOutput.time_slice_format default value is '%Y%m%d'
          end
        end

        e = Fluent::Config::Element.new('buffer', chunk_key, attr, [])
        conf.elements << e

        conf
      end

      def compat_parameters_parser(conf)
        return unless conf.elements('parse').empty?
        return if PARSER_PARAMS.keys.all?{|k| !conf.has_key?(k) }

        # TODO: warn obsolete parameters if these are deprecated
        attr = compat_parameters_copy_to_subsection_attributes(conf, PARSER_PARAMS)

        e = Fluent::Config::Element.new('parse', '', attr, [])
        conf.elements << e

        conf
      end

      def compat_parameters_formatter(conf)
        return unless conf.elements('format').empty?
        return if FORMATTER_PARAMS.keys.all?{|k| !conf.has_key?(k) }

        # TODO: warn obsolete parameters if these are deprecated
        attr = compat_parameters_copy_to_subsection_attributes(conf, FORMATTER_PARAMS)

        e = Fluent::Config::Element.new('format', '', attr, [])
        conf.elements << e

        conf
      end
    end
  end
end
