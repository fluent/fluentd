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

require 'fluent/plugin/parser'

require 'fluent/time'

module Fluent
  module Plugin
    class SyslogParser < Parser
      Plugin.register_parser('syslog', self)

      # From existence TextParser pattern
      REGEXP = /^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      # From in_syslog default pattern
      REGEXP_WITH_PRI = /^\<(?<pri>[0-9]+)\>(?<time>[^ ]* {1,2}[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[a-zA-Z0-9_\/\.\-]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      REGEXP_RFC5424 = /\A^\<(?<pri>[0-9]{1,3})\>[1-9]\d{0,2} (?<time>[^ ]+) (?<host>[^ ]+) (?<ident>[^ ]+) (?<pid>[-0-9]+) (?<msgid>[^ ]+) (?<extradata>(\[(.*)\]|[^ ])) (?<message>.+)$\z/
      REGEXP_DETECT_RFC5424 = /^\<.*\>[1-9]\d{0,2}/

      config_set_default :time_format, "%b %d %H:%M:%S"
      config_param :with_priority, :bool, default: false
      config_param :message_format, :enum, list: [:rfc3164, :rfc5424, :auto], default: :rfc3164
      config_param :rfc5424_time_format, :string, default: "%Y-%m-%dT%H:%M:%S.%L%z"

      def initialize
        super
        @mutex = Mutex.new
      end

      def configure(conf)
        super

        @regexp = case @message_format
                  when :rfc3164
                    @with_priority ? REGEXP_WITH_PRI : REGEXP
                  when :rfc5424
                    REGEXP_RFC5424
                  when :auto
                    nil
                  end
        @time_parser = time_parser_create
      end

      def patterns
        {'format' => @regexp, 'time_format' => @time_format}
      end

      def parse(text)
        if @message_format == :auto
          if REGEXP_DETECT_RFC5424.match(text)
            @regexp = REGEXP_RFC5424
            @time_parser = time_parser_create(format: @rfc5424_time_format)
          else
            @regexp = @with_priority ? REGEXP_WITH_PRI : REGEXP
            @time_parser = time_parser_create(format: @time_format)
          end
        end

        m = @regexp.match(text)
        unless m
          yield nil, nil
          return
        end

        time = nil
        record = {}

        m.names.each { |name|
          if value = m[name]
            case name
            when "pri"
              record['pri'] = value.to_i
            when "time"
              time = @mutex.synchronize { @time_parser.parse(value.gsub(/ +/, ' ')) }
              record[name] = value if @keep_time_key
            else
              record[name] = value
            end
          end
        }

        if @estimate_current_event
          time ||= Fluent::EventTime.now
        end

        yield time, record
      end
    end
  end
end
