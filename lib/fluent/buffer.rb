#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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

  #def emit(key, data, chain)
  #end

  #def keys
  #end

  #def push(key)
  #end

  #def pop(out)
  #end

  #def clear!
  #end
end


class BufferChunk
  include MonitorMixin

  def initialize(key)
    super()
    @key = key
  end

  attr_reader :key

  #def <<(data)
  #end

  #def size
  #end

  def empty?
    size == 0
  end

  #def close
  #end

  #def purge
  #end

  #def read
  #end

  #def open
  #end

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

  def emit(key, data, chain)
    key = key.to_s

    synchronize do
      top = (@map[key] ||= new_chunk(key))  # TODO generate unique chunk id

      if top.size + data.bytesize <= @buffer_chunk_limit
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

  #def new_chunk(key)
  #end

  #def resume
  #end

  #def enqueue(chunk)
  #end

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
      if !chunk.empty?
        write_chunk(chunk, out)
      end

      @queue.delete_if {|c|
        c.object_id == chunk.object_id
      }

      chunk.purge

      return !@queue.empty?
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

