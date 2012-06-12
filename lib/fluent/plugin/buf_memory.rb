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
    @data.force_encoding('ASCII-8BIT')
    super(key)
  end

  def <<(data)
    data.force_encoding('ASCII-8BIT')
    @data << data
  end

  def size
    @data.bytesize
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

  # optimize
  def write_to(io)
    io.write @data
  end

  # optimize
  def msgpack_each(&block)
    u = MessagePack::Unpacker.new
    u.feed_each(@data, &block)
  end
end


class MemoryBuffer < BasicBuffer
  Plugin.register_buffer('memory', self)

  def initialize
    super
  end

  # Overwrite default BasicBuffer#buffer_queue_limit
  # to limit total memory usage upto 512MB.
  config_set_default :buffer_queue_limit, 64

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
end


end

