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


class MemoryBufferChunk < BufferChunk
  def initialize(key, data='')
    @data = data
    super(key)
  end

  def <<(data)
    @data << data
  end

  def size
    @data.size
  end

  def close
  end

  def purge
  end

  def read
    @data
  end

  def open(&block)
    StringIO.open(@data, &block)
  end

  def write_to(io)
    io.write @data
  end
end


class MemoryBuffer < BasicBuffer
  Plugin.register_buffer('memory', self)

  def configure(conf)
    super
  end

  def before_shutdown(out)
    synchronize do
      @map.each_key {|key|
        push(key)
      }
      while pop(out)
      end
    end
  end

  def new_chunk(key)
    MemoryBufferChunk.new(key)
  end

  def resume
    return [], {}
  end

  def enqueue(chunk)
  end

  def clear!
    @queue.clear
  end
end


end

