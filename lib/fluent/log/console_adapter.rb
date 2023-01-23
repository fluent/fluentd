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

require 'console/terminal/logger'

module Fluent
  class Log
    # Async gem which is used by http_server helper switched logger mechanism to
    # Console gem which isn't complatible with Ruby's standard Logger (since
    # v1.17). This class adapts it to Fluentd's logger mechanism.
    class ConsoleAdapter < Console::Terminal::Logger
      def self.wrap(logger)
        _, level = Console::Logger::LEVELS.find { |key, value|
          if logger.level <= 0
            key == :debug
          else
            value == logger.level - 1
          end
        }
        Console::Logger.new(ConsoleAdapter.new(logger), level: level)
      end

      def initialize(logger)
        @logger = logger
        # When `verbose` is `true`, following items will be added as a prefix or
        # suffix of the subject:
        #   * Severity
        #   * Object ID
        #   * PID
        #   * Time
        # Severity and Time are added by Fluentd::Log too so they are redundant.
        # PID is the worker's PID so it's also redundant.
        # Object ID will be too verbose in usual cases.
        # So set it as `false` here to suppress redundant items.
        super(StringIO.new, verbose: false)
      end

      def call(subject = nil, *arguments, name: nil, severity: 'info', **options, &block)
        if LEVEL_TEXT.include?(severity.to_s)
          level = severity
        else
          @logger.warn("Unknown severity: #{severity}")
          level = 'warn'
        end

        @io.seek(0)
        @io.truncate(0)
        super
        @logger.send(level, @io.string.chomp)
      end
    end
  end
end
