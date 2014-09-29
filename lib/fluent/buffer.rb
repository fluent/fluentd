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
  class BufferError < StandardError
  end

  class BufferChunkLimitError < BufferError
  end

  class BufferQueueLimitError < BufferError
  end


  class Buffer
    include Configurable

    def initialize
      super
    end

    def configure(conf)
      super
    end

    def start
    end

    def shutdown
    end

    def before_shutdown(out)
    end

    def emit(key, data, chain)
      raise NotImplementedError, "Implement this method in child class"
    end

    def keys
      raise NotImplementedError, "Implement this method in child class"
    end

    def push(key)
      raise NotImplementedError, "Implement this method in child class"
    end

    def pop(out)
      raise NotImplementedError, "Implement this method in child class"
    end

    def clear!
      raise NotImplementedError, "Implement this method in child class"
    end
  end


  class BufferChunk
    include MonitorMixin

    def initialize(key)
      super()
      @key = key
    end

    attr_reader :key

    def <<(data)
      raise NotImplementedError, "Implement this method in child class"
    end

    def size
      raise NotImplementedError, "Implement this method in child class"
    end

    def empty?
      size == 0
    end

    def close
      raise NotImplementedError, "Implement this method in child class"
    end

    def purge
      raise NotImplementedError, "Implement this method in child class"
    end

    def read
      raise NotImplementedError, "Implement this method in child class"
    end

    def open
      raise NotImplementedError, "Implement this method in child class"
    end

    def write_to(io)
      open {|i|
        FileUtils.copy_stream(i, io)
      }
    end

    def msgpack_each(&block)
      open {|io|
        u = MessagePack::Unpacker.new(io)
        begin
          u.each(&block)
        rescue EOFError
        end
      }
    end
  end


  class BasicBuffer < Buffer
    include MonitorMixin

    def initialize
      super
      @map = nil # chunks to store data
      @queue = nil # chunks to be flushed
      @parallel_pop = true
    end

    def enable_parallel(b=true)
      @parallel_pop = b
    end

    # This configuration assumes plugins to send records to a remote server.
    # Local file based plugins which should provide more reliability and efficiency
    # should override buffer_chunk_limit with a larger size.
    config_param :buffer_chunk_limit, :size, :default => 8*1024*1024
    config_param :buffer_queue_limit, :integer, :default => 256

    alias chunk_limit buffer_chunk_limit
    alias chunk_limit= buffer_chunk_limit=
    alias queue_limit buffer_queue_limit
    alias queue_limit= buffer_queue_limit=

    def configure(conf)
      super
    end

    def start
      @queue, @map = resume
      @queue.extend(MonitorMixin)
    end

    def shutdown
      synchronize do
        @queue.synchronize do
          until @queue.empty?
            @queue.shift.close
          end
        end
        @map.each_pair {|key,chunk|
          chunk.close
        }
      end
    end

    def storable?(chunk, data)
      chunk.size + data.bytesize <= @buffer_chunk_limit
    end

    def emit(key, data, chain)
      key = key.to_s

      synchronize do
        top = (@map[key] ||= new_chunk(key))  # TODO generate unique chunk id

        if storable?(top, data)
          chain.next
          top << data
          return false

          ## FIXME
          #elsif data.bytesize > @buffer_chunk_limit
          #  # TODO
          #  raise BufferChunkLimitError, "received data too large"

        elsif @queue.size >= @buffer_queue_limit
          raise BufferQueueLimitError, "queue size exceeds limit"
        end

        if data.bytesize > @buffer_chunk_limit
          $log.warn "Size of the emitted data exceeds buffer_chunk_limit."
          $log.warn "This may occur problems in the output plugins ``at this server.``"
          $log.warn "To avoid problems, set a smaller number to the buffer_chunk_limit"
          $log.warn "in the forward output ``at the log forwarding server.``"
        end

        nc = new_chunk(key)  # TODO generate unique chunk id
        ok = false

        begin
          nc << data
          chain.next

          flush_trigger = false
          @queue.synchronize {
            enqueue(top)
            flush_trigger = @queue.empty?
            @queue << top
            @map[key] = nc
          }

          ok = true
          return flush_trigger
        ensure
          nc.purge unless ok
        end

      end  # synchronize
    end

    def keys
      @map.keys
    end

    def queue_size
      @queue.size
    end

    def total_queued_chunk_size
      total = 0
      @map.each_value {|c|
        total += c.size
      }
      @queue.each {|c|
        total += c.size
      }
      total
    end

    def new_chunk(key)
      raise NotImplementedError, "Implement this method in child class"
    end

    def resume
      raise NotImplementedError, "Implement this method in child class"
    end

    # enqueueing is done by #push
    # this method is actually 'enqueue_hook'
    def enqueue(chunk)
      raise NotImplementedError, "Implement this method in child class"
    end

    # get the chunk specified by key, and push it into queue
    def push(key)
      synchronize do
        top = @map[key]
        if !top || top.empty?
          return false
        end

        @queue.synchronize do
          enqueue(top)
          @queue << top
          @map.delete(key)
        end

        return true
      end  # synchronize
    end

    # shift a chunk from queue, write and purge it
    # returns boolean to indicate whether this buffer have more chunk to be flushed or not
    def pop(out)
      chunk = nil
      @queue.synchronize do
        if @parallel_pop
          chunk = @queue.find {|c| c.try_mon_enter }
          return false unless chunk
        else
          chunk = @queue.first
          return false unless chunk
          return false unless chunk.try_mon_enter
        end
      end

      begin
        # #push(key) does not push empty chunks into queue.
        # so this check is nonsense...
        if !chunk.empty?
          write_chunk(chunk, out)
        end

        empty = false
        @queue.synchronize do
          @queue.delete_if {|c|
            c.object_id == chunk.object_id
          }
          empty = @queue.empty?
        end

        chunk.purge

        return !empty
      ensure
        chunk.mon_exit
      end
    end

    def write_chunk(chunk, out)
      out.write(chunk)
    end

    def clear!
      @queue.delete_if {|chunk|
        chunk.purge
        true
      }
    end
  end
end

