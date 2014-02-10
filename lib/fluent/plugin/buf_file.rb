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
    def initialize(key, path, unique_id, mode="a+", symlink_path = nil)
      super(key)
      @path = path
      @unique_id = unique_id
      @file = File.open(@path, mode, DEFAULT_FILE_PERMISSION)
      @file.sync = true
      @size = @file.stat.size
      FileUtils.ln_sf(@path, symlink_path) if symlink_path
    end

    attr_reader :unique_id

    def <<(data)
      @file.write(data)
      @size += data.bytesize
    end

    def size
      @size
    end

    def empty?
      @size == 0
    end

    def close
      stat = @file.stat
      @file.close
      if stat.size == 0
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
      unless $platformwin
        File.rename(@path, path)
        @path = path
      else
        pos = @file.pos
        @file.close
        File.rename(@path, path)
        @path = path
        @file = File.open(@path, "a+", DEFAULT_FILE_PERMISSION)
        @file.sync = true
        @size = @file.stat.size
        @file.pos = pos
      end
    end
  end

  class FileBuffer < BasicBuffer
    Plugin.register_buffer('file', self)

    @@buffer_paths = {}

    def initialize
      require 'uri'
      super
    end

    config_param :buffer_path, :string

    attr_accessor :symlink_path

    def configure(conf)
      super

      if @@buffer_paths.has_key?(@buffer_path)
        raise ConfigError, "Other '#{@@buffer_paths[@buffer_path]}' plugin already use same buffer_path: type = #{conf['type']}, buffer_path = #{@buffer_path}"
      else
        @@buffer_paths[@buffer_path] = conf['type']
      end

      if pos = @buffer_path.index('*')
        @buffer_path_prefix = @buffer_path[0,pos]
        @buffer_path_suffix = @buffer_path[pos+1..-1]
      else
        @buffer_path_prefix = @buffer_path+"."
        @buffer_path_suffix = ".log"
      end

      if flush_at_shutdown = conf['flush_at_shutdown']
        @flush_at_shutdown = true
      else
        @flush_at_shutdown = false
      end
    end

    def start
      FileUtils.mkdir_p File.dirname(@buffer_path_prefix+"path")
      super
    end

    PATH_MATCH = /^(.*)[\._](b|q)([0-9a-fA-F]{1,32})$/

    def new_chunk(key)
      encoded_key = encode_key(key)
      path, tsuffix = make_path(encoded_key, "b")
      unique_id = tsuffix_to_unique_id(tsuffix)
      FileBufferChunk.new(key, path, unique_id, "a+", @symlink_path)
    end

    def resume
      maps = []
      queues = []

      Dir.glob("#{@buffer_path_prefix}*#{@buffer_path_suffix}") {|path|
        match = path[@buffer_path_prefix.length..-(@buffer_path_suffix.length+1)]
        if m = PATH_MATCH.match(match)
          key = decode_key(m[1])
          bq = m[2]
          tsuffix = m[3]
          timestamp = m[3].to_i(16)
          unique_id = tsuffix_to_unique_id(tsuffix)

          if bq == 'b'
            chunk = FileBufferChunk.new(key, path, unique_id, "a+")
            maps << [timestamp, chunk]
          elsif bq == 'q'
            chunk = FileBufferChunk.new(key, path, unique_id, "r")
            queues << [timestamp, chunk]
          end
        end
      }

      map = {}
      maps.sort_by {|(timestamp,chunk)|
        timestamp
      }.each {|(timestamp,chunk)|
        map[chunk.key] = chunk
      }

      queue = queues.sort_by {|(timestamp,chunk)|
        timestamp
      }.map {|(timestamp,chunk)|
        chunk
      }

      return queue, map
    end

    def enqueue(chunk)
      path = chunk.path
      mp = path[@buffer_path_prefix.length..-(@buffer_path_suffix.length+1)]

      m = PATH_MATCH.match(mp)
      encoded_key = m ? m[1] : ""
      tsuffix = m[3]
      npath = "#{@buffer_path_prefix}#{encoded_key}.q#{tsuffix}#{@buffer_path_suffix}"

      chunk.mv(npath)
    end

    def before_shutdown(out)
      if @flush_at_shutdown
        synchronize do
          @map.each_key {|key|
            push(key)
          }
          while pop(out)
          end
        end
      end
    end

    protected

    def encode_key(key)
      URI.escape(key, /[^-_.a-zA-Z0-9]/n)
    end

    def decode_key(encoded_key)
      URI.unescape(encoded_key)
    end

    def make_path(encoded_key, bq)
      now = Time.now.utc
      timestamp = ((now.to_i*1000*1000+now.usec) << 12 | rand(0xfff))
      tsuffix = timestamp.to_s(16)
      path = "#{@buffer_path_prefix}#{encoded_key}.#{bq}#{tsuffix}#{@buffer_path_suffix}"
      return path, tsuffix
    end

    def tsuffix_to_unique_id(tsuffix)
      tsuffix.scan(/../).map {|x| x.to_i(16) }.pack('C*') * 2
    end
  end
end
