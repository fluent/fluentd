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

  #def emit(data, chain)
  #end

  #def try_flush(out, force=false)
  #end

  #def clear!
  #end
end


class BufferChunk
  include MonitorMixin

  def initialize
    super()
  end

  #def <<(data)
  #end

  #def size
  #end

  def empty?
    size == 0
  end

  def close_write
  end

  #def close
  #end

  #def purge
  #end

  #def read
  #end

  #def open
  #end

  def to_str
    read
  end
end


class BasicBuffer < Buffer
  def initialize
    @chunk_limit = 1024*1024  # TODO default
    @queue_limit = 100 # TODO default
    @parallel = false
  end

  attr_accessor :queue_limit, :chunk_limit

  def enable_parallel(b=true)
    @parallel = b
  end

  def configure(conf)
    if chunk_limit = conf['buffer_chunk_limit']
      @chunk_limit = Config.size_value(chunk_limit)
    end

    if queue_limit = conf['buffer_queue_limit']
      @queue_limit = @queue_limit.to_i
    end
  end

  def start
    @queue, @top = resume_queue
    @queue.extend(MonitorMixin)
  end

  def shutdown
    @top.synchronize {
      @queue.synchronize {
        until @queue.empty?
          @queue.shift.close
        end
      }
      @top.close
    }
  end

  def emit(data, chain)
    @top.synchronize {
      if @top.size + data.size < @chunk_limit
        chain.next
        @top << data
        return false

      elsif @queue.size >= @queue_limit
        # TODO
        raise BufferError, "queue size exceeds limit"

      else
        n = new_data
        ok = false
        begin
          n << data
          chain.next
          @queue.synchronize {
            @queue << @top
          }
          @top.close_write rescue nil
          @top = n
          ok = true
        ensure
          n.purge unless ok
        end
        return true
      end
    }
  end

  #def resume_queue
  #end

  def finalize_queue(out)
    while try_flush(out, true)
    end
  end

  #def new_data
  #end

  def clear!
    @queue.clear
  end

  def try_flush(out, force)
    if @queue.empty?
      #unless force
      #  return false
      #end
      @top.synchronize {
        unless @top.empty?
          n = new_data
          ok = false
          begin
            @queue.synchronize {
              @queue << @top
            }
            @top.close_write rescue nil
            @top = n
            ok = true
          ensure
            n.purge unless ok
          end
        end
      }
    end

    chunk = nil
    @queue.synchronize {
      if @parallel
        chunk = @queue.find {|c| c.try_mon_enter }
        return false unless chunk
      else
        chunk = @queue.first
        return false unless chunk
        return false unless chunk.try_mon_enter
      end
    }

    begin
      if !chunk.empty?
        write_chunk(chunk, out)
      end

      @queue.synchronize {
        @queue.delete_if {|c|
          c.object_id == chunk.object_id
        }
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
end


end

