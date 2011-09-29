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


class ZFileBufferChunk < FileBufferChunk
  def initialize(path)
    super(path)
    @z = Zlib::Deflate.new
  end

  def <<(data)
    zdata = @z.deflate(data, Z_NO_FLUSH)
    unless zdata.empty?
      super(zdata)
    end
  end

  def close_write
    zdata = @z.flush
    unless zdata.empty?
      @file.write(zdata)
      @size += zdata.bytesize
    end
    super
  end

  def close
    @z.close
    super
  end

  def purge
    @z.close
    super
  end

  # TODO
  #def open(&block)
  #end
end


class ZFileBuffer < FileBuffer
  # TODO
  #Plugin.register_buffer('zfile', self)

  def initialize
    require 'zlib'
    super
  end

  def resume_queue
    queue = resume_queue_paths.map {|path|
      ZFileBufferChunk.new(path)
    }
    top = new_chunk
    return queue, top
  end

  def new_chunk
    path = new_chunk_path
    ZFileBufferChunk.new(path)
  end
end


end

