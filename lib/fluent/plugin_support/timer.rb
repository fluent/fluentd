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

require 'fluent/plugin_support/event_loop'

module Fluent
  module PluginSupport
    module Timer
      include Fluent::PluginSupport::EventLoop

      # interval: integer, repeat: true/false
      def timer_execute(interval: 60, repeat: true, &block)
        timer = TimerWatcher.new(interval, repeat, log, &block)
        event_loop_attach(timer)
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
