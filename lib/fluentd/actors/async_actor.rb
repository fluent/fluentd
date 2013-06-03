#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
module Fluentd
  module Actors

    module AsyncActor
      def initialize
        super
        @async = AsyncExecutor.new
      end

      # TODO not implemented yet
      #def async(&block)
      #end
    end

    class AsyncExecutor
      def initialize
        @queue = Queue.new
      end

      def submit(callable)
        f = Future.new

        task = Proc.new do
          begin
            result = callable.call
          rescue Exception => exception
          end
          f.set!(result, exception)
        end

        @queue.push(task)

        return f
      end

      def run_once
        while true
          begin
            task = @queue.pop(true)
          rescue ThreadError
            break
          end
          task.call
        end
        nil
      end
    end

  end
end
