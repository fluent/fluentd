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

require 'timeout'

module Fluent
  module Counter
    class MutexHash
      def initialize(data_store)
        @mutex = Mutex.new
        @data_store = data_store
        @mutex_hash = {}
        @thread = nil
        @cleanup_thread = CleanupThread.new(@data_store, @mutex_hash, @mutex)
      end

      def start
        @data_store.start
        @cleanup_thread.start
      end

      def stop
        @data_store.stop
        @cleanup_thread.stop
      end

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
                locks.each_value(&:unlock)
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

    class CleanupThread
      CLEANUP_INTERVAL = 60 * 15 # 15 min

      def initialize(store, mutex_hash, mutex)
        @store = store
        @mutex_hash = mutex_hash
        @mutex = mutex
        @thread = nil
        @running = false
      end

      def start
        @running = true
        @thread = Thread.new do
          while @running
            sleep CLEANUP_INTERVAL
            run_once
          end
        end
      end

      def stop
        return unless @running
        @running = false
        begin
          # Avoid waiting CLEANUP_INTERVAL
          Timeout.timeout(1) do
            @thread.join
          end
        rescue Timeout::Error
          @thread.kill
        end
      end

      private

      def run_once
        @mutex.synchronize do
          last_cleanup_at = (Time.now - CLEANUP_INTERVAL).to_i
          @mutex_hash.each do |(key, mutex)|
            v = @store.get(key, raw: true)
            next unless v
            next if last_cleanup_at < v['last_modified_at'][0] # v['last_modified_at'] = [sec, nsec]
            next unless mutex.try_lock

            @mutex_hash[key] = nil
            mutex.unlock

            # Check that a waiting thread is in a lock queue.
            # Can't get a lock here means this key is used in other places.
            # So restore a mutex value to a corresponding key.
            if mutex.try_lock
              @mutex_hash.delete(key)
              mutex.unlock
            else
              @mutex_hash[key] = mutex
            end
          end
        end
      end
    end
  end
end
