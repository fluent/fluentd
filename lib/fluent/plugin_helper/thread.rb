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

module Fluent
  module PluginHelper
    module Thread
      THREAD_DEFAULT_WAIT_SECONDS = 1

      # stop     : mark thread not to run
      # shutdown : [-]
      # close    : correct stopped threads
      # terminate: kill all threads

      attr_reader :_threads # for test driver

      def thread_current_running?
        ::Thread.current[:_fluentd_plugin_helper_thread_running] || false
      end

      def thread_create(title, *args)
        raise ArgumentError, "BUG: title must be a symbol" unless title.is_a? Symbol
        m = Mutex.new
        m.lock
        thread = ::Thread.new(*args) do |*t_args|
          m.lock # run thread after that thread is successfully set into @_thread
          ::Thread.current[:_fluentd_plugin_helper_thread_title] = title
          ::Thread.current[:_fluentd_plugin_helper_thread_running] = true
          m.unlock
          thread_exit = false
          begin
            yield *t_args
            thread_exit = true
          rescue => e
            log.warn "thread exited by unexpected error", plugin: self.class, title: title, error_class: e.class, error: e
            raise
          ensure
            unless thread_exit
              log.warn "thread doesn't exit correctly (killed or other reason)", plugin: self.class, title: title
            end
            @_threads.delete(::Thread.current.object_id)
          end
        end
        thread.abort_on_exception = true
        @_threads[thread.object_id] = thread
        m.unlock
        thread
      end

      def initialize
        super
        @_threads = {}
        @_thread_wait_seconds = THREAD_DEFAULT_WAIT_SECONDS
      end

      def stop
        super
        @_threads.each_pair do |obj_id, thread|
          thread[:_fluentd_plugin_helper_thread_running] = false
        end
      end

      def close
        @_threads.keys.each do |obj_id|
          thread = @_threads[obj_id]
          if !thread || thread.join(@_thread_wait_seconds)
            @_threads.delete(obj_id)
          end
        end

        super
      end

      def terminate
        super
        @_threads.keys.each do |obj_id|
          thread = @_threads[obj_id]
          if thread
            thread.kill
            thread.join
          end
          @_threads.delete(obj_id)
        end
      end
    end
  end
end
