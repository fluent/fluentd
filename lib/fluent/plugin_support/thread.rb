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
  module PluginSupport
    module Thread
      THREAD_DEFAULT_WAIT_SECONDS = 60

      attr_reader :_threads # for test driver

      def thread_current_running?
        ::Thread.current[:_fluentd_plugin_support_thread_running] || false
      end

      def thread_create(*args)
        thread = ::Thread.new(*args) do |*t_args|
          current = ::Thread.current
          current.abort_on_exception = true
          ::Thread.current[:_fluentd_plugin_support_thread_running] = true
          @_threads[current.object_id] = current
          begin
            yield *t_args
          rescue => e # ThreadKilledError => e
            ### TODO: log that this thread is killed forcely
            # plugin author should refer thread_current_running? method
            ### TODO:
            raise
          ensure
            @_threads.delete(::Thread.current.object_id)
          end
        end
        @_threads[thread.object_id] = thread
        thread
      end

      def initialize
        super
        @_threads = {}
      end

      def start
        super
      end

      def stop
        super
        @_threads.each_pair do |obj_id, thread|
          thread[:_fluentd_plugin_support_thread_running] = false
        end
      end

      def shutdown
        super
      end

      def close
        @_threads.keys.each do |obj_id|
          thread = @_threads[obj_id]
          if !thread && thread.join(THREAD_DEFAULT_WAIT_SECONDS)
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
