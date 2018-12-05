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

require 'fluent/config/types'
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
        "timezone" => "timekey_zone",
      }

      PARSER_PARAMS = {
        "format" => "@type",
        "types" => nil,
        "types_delimiter" => nil,
        "types_label_delimiter" => nil,
        "null_value_pattern" => "null_value_pattern",
        "null_empty_string" => "null_empty_string",
        "keys" => "keys", # CSVParser, TSVParser (old ValuesParser)
        "time_key"    => "time_key",
        "time_format" => "time_format",
        "delimiter"   => "delimiter",
        "json_parser"      => "json_parser", # JSONParser
        "label_delimiter"  => "label_delimiter", # LabeledTSVParser
        "format_firstline" => "format_firstline", # MultilineParser
        "message_key"      => "message_key", # NoneParser
        "with_priority"    => "with_priority", # SyslogParser
        # There has been no parsers which can handle timezone in v0.12
      }

      INJECT_PARAMS = {
        "include_time_key" => nil,
        "time_key"      => "time_key",
        "time_format"   => "time_format",
        "timezone"      => "timezone",
        "include_tag_key" => nil,
        "tag_key" => "tag_key",
        "localtime" => nil,
        "utc" => nil,
      }

      FORMATTER_PARAMS = {
        "format" => "@type",
        "delimiter" => "delimiter",
        "force_quotes" => "force_quotes", # CsvFormatter
        "fields" => "fields", # CsvFormatter
        "json_parser" => "json_parser", # JSONFormatter
        "label_delimiter" => "label_delimiter", # LabeledTSVFormatter
        "output_time" => "output_time", # OutFileFormatter
        "output_tag"  => "output_tag",  # OutFileFormatter
        "localtime"   => "localtime",   # OutFileFormatter
        "utc"         => "utc",         # OutFileFormatter
        "timezone"    => "timezone",    # OutFileFormatter
        "message_key" => "message_key", # SingleValueFormatter
        "add_newline" => "add_newline", # SingleValueFormatter
        "output_type" => "output_type", # StdoutFormatter
      }

      def compat_parameters_convert(conf, *types, **kwargs)
        types.each do |type|
          case type
          when :buffer
            compat_parameters_buffer(conf, **kwargs)
          when :inject
            compat_parameters_inject(conf)
          when :parser
            compat_parameters_parser(conf)
          when :formatter
            compat_parameters_formatter(conf)
          else
            raise "BUG: unknown compat_parameters type: #{type}"
          end
        end

        conf
      end

      def compat_parameters_buffer(conf, default_chunk_key: '')
        # return immediately if <buffer> section exists, or any buffer-related parameters don't exist
        return unless conf.elements('buffer').empty?
        return if (BUFFER_PARAMS.keys + BUFFER_TIME_SLICED_PARAMS.keys).all?{|k| !conf.has_key?(k) }

        # TODO: warn obsolete parameters if these are deprecated
        buffer_params = BUFFER_PARAMS.merge(BUFFER_TIME_SLICED_PARAMS)
        hash = compat_parameters_copy_to_subsection_attributes(conf, buffer_params) do |compat_key, value|
          if compat_key == 'buffer_queue_full_action' && value == 'exception'
            'throw_exception'
          else
            value
          end
        end

        chunk_key = default_chunk_key

        if conf.has_key?('time_slice_format')
          chunk_key = 'time'
          hash['timekey'] = case conf['time_slice_format']
                            when /\%S/ then 1
                            when /\%M/ then 60
                            when /\%H/ then 3600
                            when /\%d/ then 86400
                            else
                              raise Fluent::ConfigError, "time_slice_format only with %Y or %m is too long"
                            end
          if conf.has_key?('localtime') || conf.has_key?('utc')
            if conf.has_key?('localtime') && conf.has_key?('utc')
              raise Fluent::ConfigError, "both of utc and localtime are specified, use only one of them"
            elsif conf.has_key?('localtime')
              hash['timekey_use_utc'] = !(Fluent::Config.bool_value(conf['localtime']))
            elsif conf.has_key?('utc')
              hash['timekey_use_utc'] = Fluent::Config.bool_value(conf['utc'])
            end
          end
        else
          if chunk_key == 'time'
            hash['timekey'] = 86400 # TimeSliceOutput.time_slice_format default value is '%Y%m%d'
          end
        end

        e = Fluent::Config::Element.new('buffer', chunk_key, hash, [])
        conf.elements << e

        conf
      end

      def compat_parameters_inject(conf)
        return unless conf.elements('inject').empty?
        return if INJECT_PARAMS.keys.all?{|k| !conf.has_key?(k) }

        # TODO: warn obsolete parameters if these are deprecated
        hash = compat_parameters_copy_to_subsection_attributes(conf, INJECT_PARAMS)

        if conf.has_key?('include_time_key') && Fluent::Config.bool_value(conf['include_time_key'])
          hash['time_key'] ||= 'time'
          hash['time_type'] ||= 'string'
        end
        if conf.has_key?('time_as_epoch') && Fluent::Config.bool_value(conf['time_as_epoch'])
          hash['time_type'] = 'unixtime'
        end
        if conf.has_key?('localtime') || conf.has_key?('utc')
          if conf.has_key?('localtime') && conf.has_key?('utc')
            raise Fluent::ConfigError, "both of utc and localtime are specified, use only one of them"
          elsif conf.has_key?('localtime')
            hash['localtime'] = Fluent::Config.bool_value(conf['localtime'])
          elsif conf.has_key?('utc')
            hash['localtime'] = !(Fluent::Config.bool_value(conf['utc']))
            # Specifying "localtime false" means using UTC in TimeFormatter
            # And specifying "utc" is different from specifying "timezone +0000"(it's not always UTC).
            # There are difference between "Z" and "+0000" in timezone formatting.
            # TODO: add kwargs to TimeFormatter to specify "using localtime", "using UTC" or "using specified timezone" in more explicit way
          end
        end

        if conf.has_key?('include_tag_key') && Fluent::Config.bool_value(conf['include_tag_key'])
          hash['tag_key'] ||= 'tag'
        end

        e = Fluent::Config::Element.new('inject', '', hash, [])
        conf.elements << e

        conf
      end

      def compat_parameters_parser(conf)
        return unless conf.elements('parse').empty?
        return if PARSER_PARAMS.keys.all?{|k| !conf.has_key?(k) }

        # TODO: warn obsolete parameters if these are deprecated
        hash = compat_parameters_copy_to_subsection_attributes(conf, PARSER_PARAMS)

        if conf["types"]
          delimiter = conf["types_delimiter"] || ','
          label_delimiter = conf["types_label_delimiter"] || ':'
          types = {}
          conf['types'].split(delimiter).each do |pair|
            key, value = pair.split(label_delimiter, 2)
            types[key] = value
          end
          hash["types"] = JSON.dump(types)
        end

        e = Fluent::Config::Element.new('parse', '', hash, [])
        conf.elements << e

        conf
      end

      def compat_parameters_formatter(conf)
        return unless conf.elements('format').empty?
        return if FORMATTER_PARAMS.keys.all?{|k| !conf.has_key?(k) }

        # TODO: warn obsolete parameters if these are deprecated
        hash = compat_parameters_copy_to_subsection_attributes(conf, FORMATTER_PARAMS)

        if conf.has_key?('time_as_epoch') && Fluent::Config.bool_value(conf['time_as_epoch'])
          hash['time_type'] = 'unixtime'
        end

        e = Fluent::Config::Element.new('format', '', hash, [])
        conf.elements << e

        conf
      end

      def compat_parameters_copy_to_subsection_attributes(conf, params, &block)
        hash = {}
        params.each do |compat, current|
          next unless current
          if conf.has_key?(compat)
            if block_given?
              hash[current] = block.call(compat, conf[compat])
            else
              hash[current] = conf[compat]
            end
          end
        end
        hash
      end
    end
  end
end
