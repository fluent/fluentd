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

require 'serverengine'
require 'fluent/log'

module Fluent
  module Test
    class DummyLogDevice
      attr_reader :logs
      attr_accessor :flush_logs

      def initialize
        @logs = []
        @flush_logs = true
        @use_stderr = false
      end

      def reset
        @logs = [] if @flush_logs
      end

      def tty?
        false
      end

      def puts(*args)
        args.each{ |arg| write(arg + "\n") }
      end

      def write(message)
        if @use_stderr
          STDERR.write message
        end
        @logs.push message
      end

      def flush
        true
      end

      def close
        true
      end
    end

    class TestLogger < Fluent::PluginLogger
      def initialize
        @logdev = DummyLogDevice.new
        dl_opts = {}
        dl_opts[:log_level] = ServerEngine::DaemonLogger::INFO
        logger = ServerEngine::DaemonLogger.new(@logdev, dl_opts)
        log = Fluent::Log.new(logger)
        super(log)
      end

      def reset
        @logdev.reset
      end

      def logs
        @logdev.logs
      end
    end
  end
end
