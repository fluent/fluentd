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
    require 'fluentd/plugin/buffer'

    class MemoryBuffer < Buffer
      Plugin.register_buffer('memory', self)

      include MonitorMixin

      def initialize
        super
        @map = {}
        @queue = []
        @records = []
        @buffer_record_limit = 1024
        @buffer_chunk_limit = 8*1024*1024
        @buffer_queue_limit = 512
      end

      def open(&block)
        begin
          yield self
        ensure
          flush_records
        end
      end

      def append(key, data)
        if data.bytesize > @buffer_record_limit
          raise BufferRecordLimitError, "buffer record size exceeds limit"
        end
        @records << [key, data]
      end

      def flush_records
        return if @records.empty?

        synchronize do
          @records.reject! {|key,data|
            if c = @map[key]
              if c.size + data.bytesize > @buffer_chunk_limit
                @queue << c
                @map.delete(key)
                c = @map[key] = MemoryBufferChunk.new(key)
              end
            else
              c = @map[key] = MemoryBufferChunk.new(key)
            end
            c.data << data
          }

          if @queue.length > @buffer_queue_limit
            # TODO check buffer_queue_limit
          end
        end
      end

      private :flush_records

      def keys
        @map.keys
      end

      def enqueue_chunk(key)
        if c = @map.delete(key)
          @queue << c
        end
      end

      def acquire(&block)
        chunk = @queue.pop
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
