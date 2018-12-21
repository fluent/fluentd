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
      REGEXP = /^(?<time>[^ ]*\s*[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[^ :\[]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      # From in_syslog default pattern
      REGEXP_WITH_PRI = /^\<(?<pri>[0-9]+)\>(?<time>[^ ]* {1,2}[^ ]* [^ ]*) (?<host>[^ ]*) (?<ident>[^ :\[]*)(?:\[(?<pid>[0-9]+)\])?(?:[^\:]*\:)? *(?<message>.*)$/
      REGEXP_RFC5424 = /\A^(?<time>[^ ]+) (?<host>[!-~]{1,255}) (?<ident>[!-~]{1,48}) (?<pid>[!-~]{1,128}) (?<msgid>[!-~]{1,32}) (?<extradata>(?:\-|\[(.*)\]))(?: (?<message>.+))?$\z/
      REGEXP_RFC5424_WITH_PRI = /\A^\<(?<pri>[0-9]{1,3})\>[1-9]\d{0,2} (?<time>[^ ]+) (?<host>[!-~]{1,255}) (?<ident>[!-~]{1,48}) (?<pid>[!-~]{1,128}) (?<msgid>[!-~]{1,32}) (?<extradata>(?:\-|\[(.*)\]))(?: (?<message>.+))?$\z/
      REGEXP_DETECT_RFC5424 = /^\<.*\>[1-9]\d{0,2}/

      config_set_default :time_format, "%b %d %H:%M:%S"
      desc 'If the incoming logs have priority prefix, e.g. <9>, set true'
      config_param :with_priority, :bool, default: false
      desc 'Specify protocol format'
      config_param :message_format, :enum, list: [:rfc3164, :rfc5424, :auto], default: :rfc3164
      desc 'Specify time format for event time for rfc5424 protocol'
      config_param :rfc5424_time_format, :string, default: "%Y-%m-%dT%H:%M:%S.%L%z"

      def initialize
        super
        @mutex = Mutex.new
      end

      def configure(conf)
        super

        @time_parser_rfc3164 = @time_parser_rfc5424 = nil
        @time_parser_rfc5424_without_subseconds = nil
        @support_rfc5424_without_subseconds = false
        @regexp = case @message_format
                  when :rfc3164
                    class << self
                      alias_method :parse, :parse_plain
                    end
                    @with_priority ? REGEXP_WITH_PRI : REGEXP
                  when :rfc5424
                    class << self
                      alias_method :parse, :parse_plain
                    end
                    @time_format = @rfc5424_time_format unless conf.has_key?('time_format')
                    @support_rfc5424_without_subseconds = true
                    @with_priority ? REGEXP_RFC5424_WITH_PRI : REGEXP_RFC5424
                  when :auto
                    class << self
                      alias_method :parse, :parse_auto
                    end
                    @time_parser_rfc3164 = time_parser_create(format: @time_format)
                    @time_parser_rfc5424 = time_parser_create(format: @rfc5424_time_format)
                    nil
                  end
        @time_parser = time_parser_create
        @time_parser_rfc5424_without_subseconds = time_parser_create(format: "%Y-%m-%dT%H:%M:%S%z")
      end

      def patterns
        {'format' => @regexp, 'time_format' => @time_format}
      end

      def parse(text)
        # This is overwritten in configure
      end

      def parse_auto(text, &block)
        if REGEXP_DETECT_RFC5424.match(text)
          @regexp = @with_priority ? REGEXP_RFC5424_WITH_PRI : REGEXP_RFC5424
          @time_parser = @time_parser_rfc5424
          @support_rfc5424_without_subseconds = true
        else
          @regexp = @with_priority ? REGEXP_WITH_PRI : REGEXP
          @time_parser = @time_parser_rfc3164
        end
        parse_plain(text, &block)
      end

      def parse_plain(text, &block)
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
              time = @mutex.synchronize do
                time_str = value.squeeze(' ')
                begin
                  @time_parser.parse(time_str)
                rescue Fluent::TimeParser::TimeParseError => e
                  if @support_rfc5424_without_subseconds
                    log.trace(e)
                    @time_parser_rfc5424_without_subseconds.parse(time_str)
                  else
                    raise
                  end
                end
              end
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
