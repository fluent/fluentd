#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
  module Plugin

    require 'stringio'

    class MemoryBuffer < Buffer
      include MonitorMixin

      def initialize
        super
        @map = {}
        @queue = []
      end

      def open(&block)
        synchronize do
          block.call(self)
        end
      end

      def append(tag, data)
        chunk = (@map[tag] ||= MemoryBufferChunk.new(tag))
        chunk.data << [time, record].to_msgpack
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
            yield chunk
            chunk = nil
            return true
          ensure
            @queue << chunk if chunk
          end
        end
        nil
      end

      def clear
        @queue.clear
      end

      class MemoryBufferChunk < BufferChunk
        def initialize(key)
          @data = ''.force_encoding('ASCII-8BIT')
          super(key)
        end

        attr_reader :data

        def size
          @data.bytesize
        end

        def open(&block)
          yield StringIO.new(@data)
        end

        def read
          @data
        end

        def write_to(io)
          io.write(@data)
        end

        def msgpack_each(&block)
          u = MessagePack::Unpacker.new
          u.feed_each(@data, &block)
        end
      end
    end

  end
end
