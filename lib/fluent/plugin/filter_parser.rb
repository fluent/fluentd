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

require 'fluent/time'
require 'fluent/config/error'
require 'fluent/plugin/filter'
require 'fluent/plugin_helper/parser'
require 'fluent/plugin_helper/compat_parameters'

module Fluent::Plugin
  class ParserFilter < Filter
    Fluent::Plugin.register_filter('parser', self)

    helpers :parser, :compat_parameters

    config_param :key_name, :string
    config_param :reserve_data, :bool, default: false
    config_param :inject_key_prefix, :string, default: nil
    config_param :replace_invalid_sequence, :bool, default: false
    config_param :hash_value_field, :string, default: nil
    config_param :suppress_parse_error_log, :bool, default: false
    config_param :time_parse, :bool, default: true
    config_param :ignore_key_not_exist, :bool, default: false

    attr_reader :parser

    def configure(conf)
      compat_parameters_convert(conf, :parser)
      parser_config = conf.elements('parse').first
      unless parser_config
        raise Fluent::ConfigError, "<parse> section is required"
      end
      unless parser_config["@type"]
        raise Fluent::ConfigError, "parse/@type is required"
      end

      super

      @parser = parser_create(usage: 'filter_parser', conf: parser_config)
      if !@time_parse && @parser.respond_to?("time_key=".to_sym)
        # disable parse time
        @parser.time_key = nil
      end

      self
    end

    def filter_stream(tag, es)
      new_es = Fluent::MultiEventStream.new
      es.each do |time, record|
        raw_value = record[@key_name]
        if raw_value.nil?
          log.warn "#{@key_name} does not exist" unless @ignore_key_not_exist
          new_es.add(time, handle_parsed(tag, record, time, {})) if @reserve_data
          next
        end
        begin
          @parser.parse(raw_value) do |t, values|
            if values
              t ||= time
              r = handle_parsed(tag, record, t, values)
              new_es.add(t, r)
            else
              log.warn "pattern not match with data '#{raw_value}'" unless @suppress_parse_error_log
              if @reserve_data
                t = time
                r = handle_parsed(tag, record, time, {})
                new_es.add(t, r)
              end
            end
          end
        rescue Fluent::TextParser::ParserError => e
          log.warn e.message unless @suppress_parse_error_log
        rescue ArgumentError => e
          if @replace_invalid_sequence
            unless e.message.index("invalid byte sequence in") == 0
              raise
            end
            replaced_string = replace_invalid_byte(raw_value)
            @parser.parse(replaced_string) do |t, values|
              if values
                t ||= time
                r = handle_parsed(tag, record, t, values)
                new_es.add(t, r)
              else
                log.warn "pattern not match with data '#{raw_value}'" unless @suppress_parse_error_log
                if @reserve_data
                  t = time
                  r = handle_parsed(tag, record, time, {})
                  new_es.add(t, r)
                end
              end
            end
          else
            raise
          end
        rescue => e
          log.warn "parse failed #{e.message}" unless @suppress_parse_error_log
        end
      end
      new_es
    end

    private

    def handle_parsed(tag, record, t, values)
      if values && @inject_key_prefix
        values = Hash[values.map { |k, v| [@inject_key_prefix + k, v] }]
      end
      r = @hash_value_field ? {@hash_value_field => values} : values
      if @reserve_data
        r = r ? record.merge(r) : record
      end
      r
    end

    def replace_invalid_byte(string)
      replace_options = {invalid: :replace, undef: :replace, replace: '?'}
      original_encoding = string.encoding
      temporal_encoding = (original_encoding == Encoding::UTF_8 ? Encoding::UTF_16BE : Encoding::UTF_8)
      string.encode(temporal_encoding, original_encoding, replace_options).encode(original_encoding)
    end
  end
end
