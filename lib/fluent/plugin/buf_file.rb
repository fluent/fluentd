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


class FileBufferChunk < BufferChunk
  def initialize(key, path, mode="a+")
    super(key)
    @path = path
    @file = File.open(@path, mode)
    @size = @file.stat.size
  end

  def <<(data)
    @file.write(data)
    @size += data.size
    @file.flush  # TODO
  end

  def size
    @size
  end

  def empty?
    @size == 0
  end

  def close
    @file.close
    if @file.stat.size == 0
      File.unlink(@path)
    end
  end

  def purge
    @file.close
    File.unlink(@path) rescue nil  # TODO rescue?
  end

  def read
    @file.pos = 0
    @file.read
  end

  def open(&block)
    @file.pos = 0
    yield @file
  end

  attr_reader :path

  def mv(path)
    File.rename(@path, path)
    @path = path
  end
end


class FileBuffer < BasicBuffer
  Plugin.register_buffer('file', self)

  def initialize
    require 'uri'
    super
    @prefix = nil
    @suffix = nil
  end

  attr_accessor :prefix, :suffix

  def configure(conf)
    super

    if path = conf['buffer_path']
      if pos = path.index('*')
        @prefix = path[0,pos]
        @suffix = path[pos+1..-1]
      else
        @prefix = path+"."
        @suffix = ".log"
      end
    else
      raise ConfigError, "'buffer_path' parameter is required on file buffer"
    end
  end

  def start
    FileUtils.mkdir_p File.dirname(@prefix+"path")
    super
  end

  PATH_MATCH = /^(.*)_(a|r)([0-9a-fA-F]{16})$/

  def new_chunk(key)
    time16 = "%016x" % Engine.now.to_i
    encoded_key = encode_key(key)
    path = "#{@prefix}#{encoded_key}_a#{time16}#{@suffix}"
    FileBufferChunk.new(key, path)
  end

  def resume
    maps = []
    queues = []

    Dir.glob("#{@prefix}*#{@suffix}") {|path|
      match = path[@prefix.length..-(@suffix.length+1)]
      if m = PATH_MATCH.match(match)
        key = decode_key(m[1])
        ar = m[2]
        time16 = m[3].to_i(16)

        if ar == 'a'
          chunk = FileBufferChunk.new(key, path, "a+")
          maps << [time16, chunk]
        elsif ar == 'r'
          chunk = FileBufferChunk.new(key, path, "r")
          queues << [time16, chunk]
        end
      end
    }

    map = {}
    maps.sort_by {|(time16,chunk)|
      time16
    }.each {|(time16,chunk)|
      map[chunk.key] = chunk
    }

    queue = queues.sort_by {|(time16,chunk)|
      time16
    }.map {|(time16,chunk)|
      chunk
    }

    return queue, map
  end

  def enqueue(chunk)
    path = chunk.path
    mp = path[@prefix.length..-(@suffix.length+1)]

    m = PATH_MATCH.match(mp)
    encoded_key = m ? m[1] : ""
    time16 = "%016x" % Engine.now.to_i

    path = "#{@prefix}#{encoded_key}_r#{time16}#{@suffix}"
    chunk.mv(path)
  end

  def clear!
    @queue.delete_if {|chunk|
      chunk.purge
      true
    }
  end

  protected
  def encode_key(key)
		URI.encode(key, /[^-_.a-zA-Z0-9]/n)
  end

  def decode_key(encoded_key)
		URI.decode(encoded_key, /[^-_.a-zA-Z0-9]/n)
  end
end


end

