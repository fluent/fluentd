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
  def initialize(path)
    @path = path
    @file = File.open(@path, "a+")
    @size = @file.stat.size
    super()
  end

  def <<(data)
    @file.write(data)
    @size += data.size
  end

  def size
    @size
  end

  def empty?
    @size == 0
  end

  def close_write
    @file.flush
  end

  def close
    if @file.stat.size == 0
      File.unlink(@path)
    end
    @file.close
  end

  def purge
    File.unlink(@path) rescue nil  # TODO rescue?
    @file.close
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
end


class FileBuffer < BasicBuffer
  Plugin.register_buffer('file', self)

  def initialize
    super
    @prefix = nil
    @suffix = nil
  end

  attr_accessor :dir, :prefix, :suffix

  def configure(conf)
    super

    if path = conf['buffer_path']
      if pos = path.index('*')
        @prefix = path[0,pos]
        @suffix = path[pos+1..-1]
      else
        @prefix = path+"."
        @suffix = ""
      end
    else
      raise ConfigError, "'buffer_path' parameter is required on file buffer"
    end
  end

  def start
    FileUtils.mkdir_p File.dirname(@prefix)
    super
  end

  def resume_queue
    queue = resume_queue_paths.map {|path|
      FileBufferChunk.new(path)
    }
    top = queue.pop || new_data
    return queue, top
  end

  def new_data
    path = new_chunk_path
    FileBufferChunk.new(path)
  end

  protected
  def new_chunk_path
    time16 = "%016x" % Engine.now.to_i
    path = "#{@prefix}#{time16}#{@suffix}"
  end

  def resume_queue_paths
    Dir.glob("#{@prefix}*#{@suffix}").sort.select {|path|
      time16 = path[@prefix.length,8]
      time16 =~ /^[0-9a-f]{8}/
    }
  end
end


end

