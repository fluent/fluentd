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
    module EventLoop
      include Fluent::PluginSupport::Thread

      EVENT_LOOP_RUN_DEFAULT_TIMEOUT = 0.2

      def initialize
        super
        @_event_loop = Coolio::Loop.new
        @_event_loop_mutex = Mutex.new
        @_event_loop_run_timeout = EVENT_LOOP_RUN_DEFAULT_TIMEOUT
      end

      def configure(conf)
        super
      end

      def start
        super

        default_watcher = Fluent::PluginSupport::Timer::TimerWatcher.new(1, true, log) do
          # do nothing... blank watcher to run event loop
        end
        # event loop does not run here, so mutex lock is not required
        @_event_loop.attach( default_watcher )
        thread_create do
          @_event_loop.run( @_event_loop_run_timeout )
        end
      end

      def event_loop_attach(watcher)
        @_event_loop_mutex.synchronize do
          @_event_loop.attach(watcher)
        end
      end

      def shutdown
        super

        @_event_loop_mutex.synchronize do
          @_event_loop.watchers.each {|w| w.detach if w.attached? }
        end
        @_event_loop.stop
      end
    end
  end
end
