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
    @buffer_path = nil
  end

  attr_accessor :buffer_path

  def configure(conf)
    super

    if buffer_path = conf['buffer_path']
      @buffer_path = buffer_path
    end
    unless @buffer_path
      raise ConfigError, "'buffer_path' parameter is required on file buffer"
    end

    if pos = @buffer_path.index('*')
      @buffer_path_prefix = @buffer_path[0,pos]
      @buffer_path_suffix = @buffer_path[pos+1..-1]
    else
      @buffer_path_prefix = @buffer_path+"."
      @buffer_path_suffix = ".log"
    end
  end

  def start
    FileUtils.mkdir_p File.dirname(@buffer_path_prefix+"path")
    super
  end

  PATH_MATCH = /^(.*)[\._](b|q)([0-9a-fA-F]{1,16})$/

  def new_chunk(key)
    tsuffix = Engine.now.to_i.to_s(16)
    encoded_key = encode_key(key)
    path = "#{@buffer_path_prefix}#{encoded_key}.b#{tsuffix}#{@buffer_path_suffix}"
    FileBufferChunk.new(key, path)
  end

  def resume
    maps = []
    queues = []

    Dir.glob("#{@buffer_path_prefix}*#{@buffer_path_suffix}") {|path|
      match = path[@buffer_path_prefix.length..-(@buffer_path_suffix.length+1)]
      if m = PATH_MATCH.match(match)
        key = decode_key(m[1])
        bq = m[2]
        tsuffix = m[3].to_i(16)

        if bq == 'b'
          chunk = FileBufferChunk.new(key, path, "a+")
          maps << [tsuffix, chunk]
        elsif bq == 'q'
          chunk = FileBufferChunk.new(key, path, "r")
          queues << [tsuffix, chunk]
        end
      end
    }

    map = {}
    maps.sort_by {|(tsuffix,chunk)|
      tsuffix
    }.each {|(tsuffix,chunk)|
      map[chunk.key] = chunk
    }

    queue = queues.sort_by {|(tsuffix,chunk)|
      tsuffix
    }.map {|(tsuffix,chunk)|
      chunk
    }

    return queue, map
  end

  def enqueue(chunk)
    path = chunk.path
    mp = path[@buffer_path_prefix.length..-(@buffer_path_suffix.length+1)]

    m = PATH_MATCH.match(mp)
    encoded_key = m ? m[1] : ""
    tsuffix = Engine.now.to_i.to_s(16)

    path = "#{@buffer_path_prefix}#{encoded_key}.q#{tsuffix}#{@buffer_path_suffix}"
    chunk.mv(path)
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

