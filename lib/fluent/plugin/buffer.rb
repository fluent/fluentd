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

require 'monitor'
require 'fluent/configurable'
require 'fluent/system_config'

require 'fluent/plugin'

module Fluent
  module Plugin
    class BufferError < StandardError; end
    class BufferOverflowError < BufferError; end
    class BufferChunkOverflowError < BufferError; end # A record size is larger than chunk size limit

    # Buffer is to define an interface for all buffer plugins.

    MINIMUM_APPEND_ATTEMPT_SIZE = 10

    # Buffers are built on 2 element:
    #  * stage: Array of chunks under writing, specified by metadata
    #  * queue: FIFO list of chunks, which are already fulfilled, and to be flushed
    #           Queue of a Buffer instance is shared by variations of metadata
    class Buffer
      include Configurable
      include SystemConfigMixin
      include MonitorMixin

      config_section :buffer, param_name: :buffer_config, required: false, multi: false, final: true do
        config_argument :type, :string, default: nil
        config_param :chunk_bytes_limit, :size # default size is set by each buffer plugins
        config_param :total_bytes_limit, :size

        # If user specify this value and (chunk_size * queue_length) is smaller than total_size,
        # then total_size is automatically configured to that value
        config_param :queue_length_limit, :integer, default: nil

        # optional new limitations
        config_param :chunk_records_limit, :integer, default: nil

        # TODO: pipeline mode? to flush ASAP after emit
      end

      Metadata = Struct.new(:timekey, :tag, :variables)

      def initialize(logger)
        super()
        @log = logger

        @chunk_bytes_limit = nil
        @total_bytes_limit = nil
        @queue_length_limit = nil
        @chunk_records_limit = nil
      end

      def configure(plugin_id, conf)
        super

        @plugin_id = plugin_id # this is only something specified by user explicitly

        if @buffer_config
          @chunk_bytes_limit = @buffer_config.chunk_bytes_limit
          @total_bytes_limit = @buffer_config.total_bytes_limit

          @chunk_records_limit = @buffer_config.chunk_records_limit

          @queue_length_limit = @buffer_config.queue_length_limit
          if @queue_length_limit && @total_bytes_limit > @chunk_bytes_limit * @queue_length_limit
            @total_bytes_limit = @chunk_bytes_limit * @queue_length_limit
          end
        else
          @chunk_bytes_limit = self.class.const_get(:DEFAULT_CHUNK_BYTES_LIMIT)
          @total_bytes_limit = self.class.const_get(:DEFAULT_TOTAL_BYTES_LIMIT)
          @chunk_records_limit = nil
        end
      end

      def start
        super
        @stage, @queue = resume
        @queue.extend(MonitorMixin)

        @queued_num = {} # metadata => int (number of queued)
        @queue.each do |chunk|
          @queued_num[chunk.metadata] ||= 0
          @queued_num[chunk.metadata] += 1
        end

        @dequeued = {} # unique_id => chunk

        @stage_size = @queue_size = 0
        @metadata_list = [] # keys of @stage
      end

      def storable?
        @total_size_limit > @stage_size + @queue_size
      end

      # def used?(ratio) # TODO: for back pressure feature
      #   @total_size_limit * ratio > @stage_size + @queue_size
      # end

      def resume
        raise NotImplementedError, "Implement this method in child class"
      end

      def generate_chunk(metadata)
        raise NotImplementedError, "Implement this method in child class"
      end

      def metadata_list
        synchronize do
          @metadata_list.dup
        end
      end

      def metadata(timekey: nil, tag: nil, key_value_pairs={})
        # TODO cache metadata not to new it
        meta = if key_value_pairs.empty?
                 Metadata.new(timekey, tag, [])
               else
                 variables = key_value_pairs.keys.sort.map{|k| key_value_pairs[k] }
                 Metadata.new(timekey, tag, variables)
               end
        synchronize do
          if i = @metadata_list.index(meta)
            @metadata_list[i]
          else
            @metadata_list << meta
            meta
          end
        end
      end

      # metadata MUST have consistent object_id for each variation
      # data MUST be Array of serialized events
      def emit(metadata, data)
        return if data.size < 1
        raise BufferOverflowError unless storable?

        stored = false
        data_size = data.size

        # the case whole data can be stored in staged chunk: almost all emits will success
        chunk = synchronize { @stage[metadata] ||= generate_chunk(metadata) }
        chunk.synchronize do
          begin
            chunk.append(data)
            unless size_over?(chunk)
              chunk.commit
              stored = true
            end
          ensure
            chunk.rollback
          end
        end
        return if stored

        emit_step_by_step(metadata, data)
      end

      def staged_chunk_test(metadata) # block
        synchronize do
          chunk = @stage[metadata]
          yield chunk
        end
      end

      def queued?(metadata=nil)
        synchronize do
          return !@queue.empty? unless metadata
          n = @queued_num[metadata]
          n && n.nonzero?
        end
      end

      def enqueue_chunk(metadata)
        synchronize do
          chunk = @stage.delete(metadata)
          if chunk && !chunk.empty?
            @queue << chunk
            @queued_num[metadata] = @queued_num.fetch(metadata, 0) + 1
          end
          nil
        end
      end

      def enqueue_all
        synchronize do
          @stage.keys.each do |metadata|
            chunk = @stage.delete(metadata)
            if chunk && !chunk.empty?
              @queue << chunk
              @queued_num[metadata] = @queued_num.fetch(metadata, 0) + 1
            end
          end
        end
      end

      def dequeue_chunk
        return nil if @queue.empty?
        synchronize do
          chunk = @queue.shift
          return nil unless chunk # queue is empty
          @dequeued[chunk.unique_id] = chunk
          @queued_num[chunk.metadata] -= 1 # BUG if nil, 0 or subzero
          chunk
        end
      end

      def takeback_chunk(chunk_id)
        synchronize do
          chunk = @dequeued.delete(chunk_id)
          return false unless chunk # already purged by other thread
          @queue.unshift(chunk)
          @queued_num[chunk.metadata] += 1 # BUG if nil
          true
        end
      end

      def purge_chunk(chunk_id)
        synchronize do
          chunk = @dequeued.delete(chunk_id)
          begin
            if chunk
              chunk.purge
              if !@stage[chunk.metadata] && (!@queued_num[chunk.metadata] || @queued_num[chunk.metadata] < 1)
                @metadata_list.delete(chunk.metadata)
              end
            end
          rescue => e
            @log.error "failed to purge buffer chunk", chunk_id: chunk_id, error_class: e.class, error: e
          end
          nil
        end
      end

      def queued_records
        synchronize { @queue.reduce(0){|r, chunk| r + chunk.records } }
      end

      def clear!
        synchronize do
          until @queue.empty?
            begin
              @queue.shift.purge
            rescue => e
              @log.error "unexpected error while clearing buffer queue", error_class: e.class, error: e
            end
          end
        end
      end

      def stop
      end

      def shutdown
      end

      def close
        synchronize do
          @dequeued.synchronize do
            @dequeued.each_pair do |chunk_id, chunk|
              chunk.close
            end
          end
          @queue.synchronize do
            until @queue.empty?
              @queue.shift.close
            end
          end
          @stage.each_pair do |metadata, chunk|
            chunk.close
          end
        end
      end

      def terminate
        @stage = @queue = nil
      end

      def size_over?(chunk)
        chunk.size > @chunk_bytes_limit || (@chunk_records_limit && chunk.records > @chunk_records_limit)
      end

      def emit_step_by_step(metadata, data)
        attempt_size = data.size / 3

        synchronize do # critical section for buffer (stage/queue)
          while data.size > 0
            if attempt_size < MINIMUM_APPEND_ATTEMPT_SIZE
              attempt_size = MINIMUM_APPEND_ATTEMPT_SIZE
            end

            chunk = @stage[metadata]
            unless chunk
              chunk = @stage[metadata] = generate_chunk(metadata)
            end

            chunk.synchronize do # critical section for chunk (chunk append/commit/rollback)
              begin
                empty_chunk = chunk.empty?

                attempt = data.slice(0, attempt_size)
                chunk.append(attempt)

                if size_over?(chunk)
                  chunk.rollback

                  if attempt_size <= MINIMUM_APPEND_ATTEMPT_SIZE
                    if empty_chunk # record is too large even for empty chunk
                      raise BufferChunkOverflowError, "minimum append butch exceeds chunk bytes limit"
                    end
                    # no more records for this chunk -> enqueue -> to be flushed
                    enqueue_chunk(metadata) # `chunk` will be removed from stage
                    attempt_size = data.size # fresh chunk may have enough space
                  else
                    # whole data can be processed by twice operation
                    #  ( by using apttempt /= 2, 3 operations required for odd numbers of data)
                    attempt_size = (attempt_size / 2) + 1
                  end

                  next
                end

                chunk.commit
                data.slice!(0, attempt_size)
                # same attempt size
                nil # discard return value of data.slice!() immediately
              ensure
                chunk.rollback
              end
            end
          end
        end
        nil
      end # emit_step_by_step
    end
  end
end
