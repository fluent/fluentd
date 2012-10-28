#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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
module Fluentd
  module Plugins

    class MemoryBuffer < Buffers::BasicBuffer
      class MemoryBufferChunk < Chunks::BasicChunk
        def initialize(tag)
          @tag = tag
          @data = ''.force_encoding('ASCII-8BIT')
        end

        attr_reader :tag, :data

        def size
          @data.bytesize
        end

        def each(&block)
          u = MessagePack::Unpacker.new
          begin
            u.feed_each(@data, &block)
          rescue EOFError
          end
        end

        def read
          @data
        end
      end

      def initialize
        super
        @mutex = Mutex.new
        @map = {}
        @queue = []
      end

      def open(tag)
        m = @mutex
        m.lock
        return Writers::SimpleWriter.new(self, tag) { m.unlock }
      end

      def append(tag, time, record)
        chunk = (@map[tag] ||= MemoryBufferChunk.new(tag))
        chunk.data << [time, record].to_msgpack
      end

      def write(tag, chunk)
        chunk = (@map[tag] ||= MemoryBufferChunk.new(tag))
        chunk.data << chunk.read
      end

      def acquire(&block)
        if @queue.empty?
          # FIXME
          chunk = @map.delete(@map.keys.first)
        else
          chunk = @queue.pop
        end
        if chunk
          begin
            yield chunk.tag, chunk
            chunk = nil
            return true
          ensure
            @queue << chunk if chunk
          end
        end
        nil
      end

      def clear!
        @queue.clear
      end
    end

  end
end

