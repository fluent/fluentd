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


class Buffer
  def initialize
  end

  def configure(conf)
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
end


class BasicBuffer < Buffer
  include MonitorMixin

  def initialize
    super()
    @chunk_limit = 1024*1024  # TODO default
    @queue_limit = 100 # TODO default
    @parallel = false
  end

  attr_accessor :queue_limit, :chunk_limit

  def configure(conf)
    if chunk_limit = conf['buffer_chunk_limit']
      @chunk_limit = Config.size_value(chunk_limit)
    end

    if queue_limit = conf['buffer_queue_limit']
      @queue_limit = @queue_limit.to_i
    end
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

      if top.size + data.size <= @chunk_limit
        chain.next
        top << data
        return false

      elsif data.size > @chunk_limit
        # TODO
        raise BufferError, "received data too large"

      elsif @queue.size >= @queue_limit
        # TODO
        raise BufferError, "queue size exceeds limit"
      end

      nc = new_chunk(key)  # TODO generate unique chunk id
      ok = false

      begin
        nc << data
        chain.next

        @queue.synchronize {
          enqueue(top)
          @queue << top
          @map[key] = nc
        }

        ok = true
        return @queue.size == 1
      ensure
        nc.purge unless ok
      end

    end  # synchronize
  end

  def keys
    @map.keys
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

      @queue.synchronize {
        enqueue(top)
        @queue << top
        @map.delete(key)
      }

      return true
    end  # synchronize
  end

  def pop(out)
    chunk = nil
    synchronize do
      if @parallel
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

      synchronize do
        @queue.delete_if {|c|
          c.object_id == chunk.object_id
        }
      end

      chunk.purge

      return !@queue.empty?
    ensure
      chunk.mon_exit
    end
  end

  def write_chunk(chunk, out)
    out.write(chunk)
  end
end


end

