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
require 'fluent/plugin_helper/thread'
require 'fluent/clock'

module Fluent
  module PluginHelper
    module EventLoop
      # Currently this plugin helper is only for other helpers, not plugins.
      # there's no way to create customized watchers to attach event loops.
      include Fluent::PluginHelper::Thread

      # stop     : [-]
      # shutdown : detach all event watchers on event loop
      # close    : stop event loop
      # terminate: initialize internal state

      EVENT_LOOP_RUN_DEFAULT_TIMEOUT = 0.5
      EVENT_LOOP_SHUTDOWN_TIMEOUT = 5

      attr_reader :_event_loop # for tests

      def event_loop_attach(watcher)
        @_event_loop_mutex.synchronize do
          @_event_loop.attach(watcher)
          @_event_loop_attached_watchers << watcher
          watcher
        end
      end

      def event_loop_detach(watcher)
        if watcher.attached?
          watcher.detach
        end
        @_event_loop_mutex.synchronize do
          @_event_loop_attached_watchers.delete(watcher)
        end
      end

      def event_loop_wait_until_start
        sleep(0.1) until event_loop_running?
      end

      def event_loop_wait_until_stop
        timeout_at = Fluent::Clock.now + EVENT_LOOP_SHUTDOWN_TIMEOUT
        sleep(0.1) while event_loop_running? && Fluent::Clock.now < timeout_at
        if @_event_loop_running
          puts "terminating event_loop forcedly"
          caller.each{|bt| puts "\t#{bt}" }
          @_event_loop.stop rescue nil
          @_event_loop_running = true
        end
      end

      def event_loop_running?
        @_event_loop_running
      end

      def initialize
        super
        @_event_loop = Coolio::Loop.new
        @_event_loop_running = false
        @_event_loop_mutex = Mutex.new
        # plugin MAY configure loop run timeout in #configure
        @_event_loop_run_timeout = EVENT_LOOP_RUN_DEFAULT_TIMEOUT
        @_event_loop_attached_watchers = []
      end

      def start
        super

        # event loop does not run here, so mutex lock is not required
        thread_create :event_loop do
          begin
            default_watcher = DefaultWatcher.new
            event_loop_attach(default_watcher)
            @_event_loop_running = true
            @_event_loop.run(@_event_loop_run_timeout) # this method blocks
          ensure
            @_event_loop_running = false
          end
        end
      end

      def shutdown
        @_event_loop_mutex.synchronize do
          @_event_loop_attached_watchers.reverse.each do |w|
            if w.attached?
              begin
                w.detach
              rescue => e
                log.warn "unexpected error while detaching event loop watcher", error: e
              end
            end
          end
        end

        super
      end

      def after_shutdown
        timeout_at = Fluent::Clock.now + EVENT_LOOP_SHUTDOWN_TIMEOUT
        @_event_loop_mutex.synchronize do
          @_event_loop.watchers.reverse.each do |w|
            begin
              w.detach
            rescue => e
              log.warn "unexpected error while detaching event loop watcher", error: e
            end
          end
        end
        while @_event_loop_running
          if Fluent::Clock.now >= timeout_at
            log.warn "event loop does NOT exit until hard timeout."
            raise "event loop does NOT exit until hard timeout." if @under_plugin_development
            break
          end
          sleep 0.1
        end

        super
      end

      def close
        if @_event_loop_running
          begin
            @_event_loop.stop # we cannot check loop is running or not
          rescue RuntimeError => e
            raise unless e.message == 'loop not running'
          end
        end

        super
      end

      def terminate
        @_event_loop = nil
        @_event_loop_running = false
        @_event_loop_mutex = nil
        @_event_loop_run_timeout = nil

        super
      end

      # watcher to block to run event loop until shutdown
      class DefaultWatcher < Coolio::TimerWatcher
        def initialize
          super(1, true) # interval: 1, repeat: true
        end
        # do nothing
        def on_timer; end
      end
    end
  end
end
