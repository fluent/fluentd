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

require 'fluent/plugin_helper/event_loop'
require 'set'

module Fluent
  module PluginHelper
    module Timer
      include Fluent::PluginHelper::EventLoop

      # stop     : turn checker into false (callbacks not called anymore)
      # shutdown : [-]
      # close    : [-]
      # terminate: [-]

      attr_reader :_timers # for tests

      # interval: integer/float, repeat: true/false
      def timer_execute(title, interval, repeat: true, &block)
        raise ArgumentError, "BUG: title must be a symbol" unless title.is_a? Symbol
        raise ArgumentError, "BUG: block not specified for callback" unless block_given?
        checker = ->(){ @_timer_running }
        detacher = ->(watcher){ event_loop_detach(watcher) }
        timer = TimerWatcher.new(title, interval, repeat, log, checker, detacher, &block)
        @_timers << title
        event_loop_attach(timer)
        timer
      end

      def timer_running?
        defined?(@_timer_running) && @_timer_running
      end

      def initialize
        super
        @_timers ||= Set.new
      end

      def start
        super
        @_timer_running = true
      end

      def stop
        super
        @_timer_running = false
      end

      def terminate
        super
        @_timers = nil
      end

      class TimerWatcher < Coolio::TimerWatcher
        def initialize(title, interval, repeat, log, checker, detacher, &callback)
          @title = title
          @callback = callback
          @repeat = repeat
          @log = log
          @checker = checker
          @detacher = detacher
          super(interval, repeat)
        end

        def on_timer
          @callback.call if @checker.call
        rescue => e
          @log.error "Unexpected error raised. Stopping the timer.", title: @title, error: e
          @log.error_backtrace
          detach
          @log.error "Timer detached.", title: @title
        ensure
          @detacher.call(self) unless @repeat
        end
      end
    end
  end
end
