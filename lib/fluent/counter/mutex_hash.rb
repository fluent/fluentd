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
  module Counter
    class MutexHash
      def initialize(data_store)
        @mutex = Mutex.new
        @data_store = data_store
        @mutex_hash = {}
        # @thread = nil
        # @cleaup_thread = CleaupThread.new(@store, @dict, @dict_mutex)
      end

      # def start
      #   @cleaup_thread.start
      # end

      # def stop
      #   @cleaup_thread.stop
      # end

      def synchronize(*keys)
        return if keys.empty?

        locks = {}
        loop do
          @mutex.synchronize do
            keys.each do |key|
              mutex = @mutex_hash[key]
              unless mutex
                v = Mutex.new
                @mutex_hash[key] = v
                mutex = v
              end

              if mutex.try_lock
                locks[key] = mutex
              else
                locks.values.each(&:unlock)
                locks = {}          # flush locked keys
                break
              end
            end
          end

          next if locks.empty?      # failed to lock all keys

          locks.each do |(k, v)|
            yield @data_store, k
            v.unlock
          end
          break
        end
      end

      def synchronize_keys(*keys)
        return if keys.empty?
        keys = keys.dup

        while key = keys.shift
          @mutex.lock

          mutex = @mutex_hash[key]
          unless mutex
            v = Mutex.new
            @mutex_hash[key] = v
            mutex = v
          end

          if mutex.try_lock
            @mutex.unlock
            yield @data_store, key
            mutex.unlock
          else
            # release global lock
            @mutex.unlock
            keys.push(key)          # failed lock, retry this key
          end
        end
      end
    end
  end
end
