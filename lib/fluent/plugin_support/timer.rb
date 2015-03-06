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

require 'cool.io'
require 'fluent/plugin_support/thread'

module Fluent
  module PluginSupport
    module Timer
      include Fluent::PluginSupport::Thread

      def initialize
        super
        @_timer_loop = nil
        @_timer_mutex = Mutex.new
      end

      def start
        super

        @_timer_loop = Coolio::Loop.new
        default_watcher = TimerWatcher.new(1, true, log) do
          # do nothing
        end
        @_timer_loop.attach(default_watcher)

        thread_create do
          @_timer_loop.run
        end
      end

      def shutdown
        @_timer_loop.watchers.each {|w| w.detach if w.attached? }
        @_timer_loop.stop

        super
      end

      # interval: integer, repeat: true/false
      def timer_execute(interval: 60, repeat: true, &block)
        timer = TimerWatcher.new(interval, repeat, log, &block)
        @_timer_mutex.synchronize do
          @_timer_loop.attach(timer)
        end
      end

      class TimerWatcher < Coolio::TimerWatcher
        def initialize(interval, repeat, log, &callback)
          @callback = callback
          @log = log
          super(interval, repeat)
        end

        def on_timer
          @callback.call
        rescue => e
          @log.error "Something wrong in timer callback", error: e, error_class: e.class
          @log.error_backtrace
        end
      end
    end
  end
end
