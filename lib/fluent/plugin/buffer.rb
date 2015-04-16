#
# Fluentd
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
  module Plugin
    class BufferError < StandardError; end
    class BufferChunkLimitError < BufferError; end
    class BufferQueueLimitError < BufferError; end

    # Buffer is to define an interface for all buffer plugins.
    # Use BasicBuffer as a superclass for 3rd party buffer plugins.

    DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024 # 8MB for memory
    DEFAULT_QUEUE_LENGTH = 256 # (8MB * 256 ==) 2GB for memory

    # Buffers are built on 2 element:
    #  * stage: Array of chunks under writing, specified by metadata
    #  * queue: FIFO list of chunks, which are already fulfilled, and to be flushed
    #           Queue of a Buffer instance is shared by variations of metadata
    class Buffer
      include Configurable

      config_section :buffer, param_name: :buffer_config, required: false, multi: false do
        config_param :chunk_size, :size, default: DEFAULT_CHUNK_SIZE
        config_param :total_size, :size, default: DEFAULT_CHUNK_SIZE * DEFAULT_QUEUE_LENGTH

        config_param :flush_interval, :time, default: nil

        # If user specify this value and (chunk_size * queue_length) is smaller than total_size,
        # then total_size is automatically configured to that value
        config_param :queue_length, :integer, default: nil

        # optional new limitations
        config_param :chunk_records, :integer, default: nil

        # TODO: pipeline mode? to flush ASAP after emit
      end

      def initialize(logger)
        super()
        @log = logger

        @chunk_size = nil
        @chunk_records = nil

        @total_size = nil
        @queue_length = nil

        @flush_interval = nil
      end

      def configure(conf)
        super

        if @buffer_config
          @chunk_size = @buffer_config.chunk_size
          @chunk_records = @buffer_config.chunk_records
          @total_size = @buffer_config.total_size
          @queue_length = @buffer_config.queue_length
          if @queue_length && @total_size > @chunk_size * @queue_length
            @total_size = @chunk_size * @queue_length
          end
          @flush_interval = @buffer_config.flush_interval
        else
          @chunk_size = DEFAULT_CHUNK_SIZE
          @total_size = DEFAULT_CHUNK_SIZE * DEFAULT_QUEUE_LENGTH
          @queue_length = DEFAULT_QUEUE_LENGTH
        end
      end

      def allow_concurrent_pop?
        raise NotImplementedError, "Implement this method in child class"
      end

      def start
        super
      end

      def emit(data, metadata)
        raise NotImplementedError, "Implement this method in child class"
      end

      def enqueue_chunk(key)
        raise NotImplementedError, "Implement this method in child class"
      end

      def dequeue_chunk
        raise NotImplementedError, "Implement this method in child class"
      end

      def purge_chunk(chunk_id)
        raise NotImplementedError, "Implement this method in child class"
      end

      def clear!
        raise NotImplementedError, "Implement this method in child class"
      end

      def stop
      end

      def before_shutdown(out)
      end

      def shutdown
      end

      def close
      end

      def terminate
      end
    end
  end
end
