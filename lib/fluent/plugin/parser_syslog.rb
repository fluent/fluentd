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

      config_set_default :time_format, "%b %d %H:%M:%S"
      config_param :with_priority, :bool, default: false

      def initialize
        super
        @mutex = Mutex.new
      end

      def configure(conf)
        super

        @regexp = @with_priority ? REGEXP_WITH_PRI : REGEXP
        @time_parser = time_parser_create
      end

      def patterns
        {'format' => @regexp, 'time_format' => @time_format}
      end

      def parse(text)
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
