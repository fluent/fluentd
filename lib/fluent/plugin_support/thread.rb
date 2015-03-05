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
      def initialize
        super
        @_threads = {}
      end

      def thread_create(*args)
        thread = ::Thread.new(*args) do |*t_args|
          current = ::Thread.current
          current.abort_on_exception = true
          @_threads[current.object_id] = current
          begin
            yield *t_args
          ensure
            @_threads.delete(::Thread.current.object_id)
          end
        end
        @_threads[thread.object_id] = thread
        thread
      end
    end
  end
end
