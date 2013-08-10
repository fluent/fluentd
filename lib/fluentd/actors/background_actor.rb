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

    require_relative 'future'

    module BackgroundActor
      def initialize
        super
        @background = ThreadExecutor.new
      end

      def background(&block)
        @background.submit(block)
      end
    end

    class ThreadExecutor
      def submit(callable)
        f = Future.new

        Thread.new do
          begin
            result = callable.call
          rescue => exception
          end
          f.set!(result, exception)
        end

        return f
      end
    end

=begin
    class CachedThreadPoolExecutor
      def initialize
        @ready_threads = []
        @all_threads = []
      end

      def submit(callable)
        f = Future.new

        task = lambda do
          begin
            result = callable.call
          rescue => exception
          end
          f.set!(result, exception)
        end

        t = @ready_threads.pop
        unless t
          t = CachedThread.new {|th| @ready_threads << th }
          @all_threads << t
        end

        t.submit(task)

        return f
      end

      class CachedThread < Thread
        def initialize(&ready_callback)
          @ready_callback = ready_callback
          super(self, &:main)

          @mutex = Mutex.new
          @cond = ConditionVariable.new
          @task = nil
        end

        def main
          while true
            @mutex.synchronize do
              @ready_callback.call(self)

              while true
                t = @task
                if t
                  @task = nil
                  break
                end
                @cond.wait(@mutex)
              end
            end

            return if t == :finish

            t.run
          end
        end

        def submit(task)
          @mutex.synchronize do
            @task = task
            @cond.broadcast
          end
        end
      end
      # TODO
    end
=end

  end
end
