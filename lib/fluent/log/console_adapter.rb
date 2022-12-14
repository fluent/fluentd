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

require 'console/serialized/logger'

module Fluent
  class Log
    class ConsoleAdapter < Console::Serialized::Logger
      def self.wrap(logger)
        Console::Logger.new(ConsoleAdapter.new(logger))
      end

      def initialize(logger)
        @logger = logger
        super(@logger)
      end

      def call(subject = nil, *arguments, severity: 'info', **options, &block)
        if LEVEL_TEXT.include?(severity.to_s)
          level = severity
        else
          @logger.warn "Unknown severity: #{severity}"
          level = 'warn'
        end

        args = arguments.dup
        args.unshift("#{subject}") if subject

        if block_given?
          if block.arity.zero?
            args << yield
          else
            buffer = StringIO.new
            yield buffer
            args << buffer.string
          end
        end

        exception = find_exception(args)
        args.delete(exception) if exception

        @logger.send(severity, args.join(" "))
        if exception && @logger.respond_to?("#{level}_backtrace")
          @logger.send("#{level}_backtrace", exception)
        end
      end
    end
  end
end
