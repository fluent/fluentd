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

require 'fluent/clock'

module Fluent
  module PluginHelper
    module Thread
      THREAD_DEFAULT_WAIT_SECONDS = 1
      THREAD_SHUTDOWN_HARD_TIMEOUT_IN_TESTS = 100 # second

      # stop     : mark callback thread as stopped
      # shutdown : [-]
      # close    : correct stopped threads
      # terminate: kill all threads

      attr_reader :_threads # for test driver

      def thread_current_running?
        # checker for code in callback of thread_create
        ::Thread.current[:_fluentd_plugin_helper_thread_running] || false
      end

      def thread_wait_until_start
        until @_threads_mutex.synchronize{ @_threads.values.reduce(true){|r,t| r && t[:_fluentd_plugin_helper_thread_started] } }
          sleep 0.1
        end
      end

      def thread_wait_until_stop
        timeout_at = Fluent::Clock.now + THREAD_SHUTDOWN_HARD_TIMEOUT_IN_TESTS
        until @_threads_mutex.synchronize{ @_threads.values.reduce(true){|r,t| r && !t[:_fluentd_plugin_helper_thread_running] } }
          break if Fluent::Clock.now > timeout_at
          sleep 0.1
        end
        @_threads_mutex.synchronize{ @_threads.values }.each do |t|
          if t.alive?
            puts "going to kill the thread still running: #{t[:_fluentd_plugin_helper_thread_title]}"
            t.kill rescue nil
            t[:_fluentd_plugin_helper_thread_running] = false
          end
        end
      end

      # Ruby 2.2.3 or earlier (and all 2.1.x) cause bug about Threading ("Stack consistency error")
      #  by passing splatted argument to `yield`
      # https://bugs.ruby-lang.org/issues/11027
      # We can enable to pass arguments after expire of Ruby 2.1 (& older 2.2.x)
      # def thread_create(title, *args)
      #   Thread.new(*args) do |*t_args|
      #     yield *t_args
      def thread_create(title)
        raise ArgumentError, "BUG: title must be a symbol" unless title.is_a? Symbol
        raise ArgumentError, "BUG: callback not specified" unless block_given?
        m = Mutex.new
        m.lock
        thread = ::Thread.new do
          m.lock # run thread after that thread is successfully set into @_threads
          m.unlock
          thread_exit = false
          ::Thread.current[:_fluentd_plugin_helper_thread_title] = title
          ::Thread.current[:_fluentd_plugin_helper_thread_started] = true
          ::Thread.current[:_fluentd_plugin_helper_thread_running] = true
          begin
            yield
            thread_exit = true
          rescue Exception => e
            log.warn "thread exited by unexpected error", plugin: self.class, title: title, error: e
            thread_exit = true
            raise
          ensure
            @_threads_mutex.synchronize do
              @_threads.delete(::Thread.current.object_id)
            end
            ::Thread.current[:_fluentd_plugin_helper_thread_running] = false
            if ::Thread.current.alive? && !thread_exit
              log.warn "thread doesn't exit correctly (killed or other reason)", plugin: self.class, title: title, thread: ::Thread.current, error: $!
            end
          end
        end
        thread.abort_on_exception = true
        @_threads_mutex.synchronize do
          @_threads[thread.object_id] = thread
        end
        m.unlock
        thread
      end

      def thread_exist?(title)
        @_threads.values.select{|thread| title == thread[:_fluentd_plugin_helper_thread_title] }.size > 0
      end

      def thread_started?(title)
        t = @_threads.values.select{|thread| title == thread[:_fluentd_plugin_helper_thread_title] }.first
        t && t[:_fluentd_plugin_helper_thread_started]
      end

      def thread_running?(title)
        t = @_threads.values.select{|thread| title == thread[:_fluentd_plugin_helper_thread_title] }.first
        t && t[:_fluentd_plugin_helper_thread_running]
      end

      def initialize
        super
        @_threads_mutex = Mutex.new
        @_threads = {}
        @_thread_wait_seconds = THREAD_DEFAULT_WAIT_SECONDS
      end

      def stop
        super
        wakeup_threads = []
        @_threads_mutex.synchronize do
          @_threads.each_value do |thread|
            thread[:_fluentd_plugin_helper_thread_running] = false
            wakeup_threads << thread if thread.alive? && thread.status == "sleep"
          end
        end
        wakeup_threads.each do |thread|
          if thread.alive?
            thread.wakeup
          end
        end
      end

      def after_shutdown
        super
        wakeup_threads = []
        @_threads_mutex.synchronize do
          @_threads.each_value do |thread|
            wakeup_threads << thread if thread.alive? && thread.status == "sleep"
          end
        end
        wakeup_threads.each do |thread|
          thread.wakeup if thread.alive?
        end
      end

      def close
        @_threads_mutex.synchronize{ @_threads.keys }.each do |obj_id|
          thread = @_threads[obj_id]
          if !thread || thread.join(@_thread_wait_seconds)
            @_threads_mutex.synchronize{ @_threads.delete(obj_id) }
          end
        end

        super
      end

      def terminate
        super
        @_threads_mutex.synchronize{ @_threads.keys }.each do |obj_id|
          thread = @_threads[obj_id]
          log.warn "killing existing thread", thread: thread
          thread.kill if thread
        end
        @_threads_mutex.synchronize{ @_threads.keys }.each do |obj_id|
          thread = @_threads[obj_id]
          thread.join
          @_threads_mutex.synchronize{ @_threads.delete(obj_id) }
        end
        @_thread_wait_seconds = nil
      end
    end
  end
end
