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

require 'fluent/plugin/base'
require 'fluent/plugin/owned_by_mixin'
require 'fluent/unique_id'

require 'monitor'

module Fluent
  module Plugin
    class Buffer < Base
      include OwnedByMixin
      include UniqueId::Mixin
      include MonitorMixin

      class BufferError < StandardError; end
      class BufferOverflowError < BufferError; end
      class BufferChunkOverflowError < BufferError; end # A record size is larger than chunk size limit

      MINIMUM_APPEND_ATTEMPT_RECORDS = 10

      DEFAULT_CHUNK_LIMIT_SIZE =   8 * 1024 * 1024 # 8MB
      DEFAULT_TOTAL_LIMIT_SIZE = 512 * 1024 * 1024 # 512MB, same with v0.12 (BufferedOutput + buf_memory: 64 x 8MB)

      DEFAULT_CHUNK_FULL_THRESHOLD = 0.95

      configured_in :buffer

      # TODO: system total buffer limit size in bytes by SystemConfig

      config_param :chunk_limit_size, :size, default: DEFAULT_CHUNK_LIMIT_SIZE
      config_param :total_limit_size, :size, default: DEFAULT_TOTAL_LIMIT_SIZE

      # If user specify this value and (chunk_size * queue_length) is smaller than total_size,
      # then total_size is automatically configured to that value
      config_param :queue_length_limit, :integer, default: nil

      # optional new limitations
      config_param :chunk_records_limit, :integer, default: nil

      # if chunk size (or records) is 95% or more after #write, then that chunk will be enqueued
      config_param :chunk_full_threshold, :float, default: DEFAULT_CHUNK_FULL_THRESHOLD

      Metadata = Struct.new(:timekey, :tag, :variables)

      # for tests
      attr_accessor :stage_size, :queue_size
      attr_reader :stage, :queue, :dequeued, :queued_num

      def initialize
        super

        @chunk_limit_size = nil
        @total_limit_size = nil
        @queue_length_limit = nil
        @chunk_records_limit = nil

        @stage = {}    #=> Hash (metadata -> chunk) : not flushed yet
        @queue = []    #=> Array (chunks)           : already flushed (not written)
        @dequeued = {} #=> Hash (unique_id -> chunk): already written (not purged)
        @queued_num = {} # metadata => int (number of queued chunks)

        @stage_size = @queue_size = 0
        @metadata_list = [] # keys of @stage
      end

      def persistent?
        false
      end

      def configure(conf)
        super

        unless @queue_length_limit.nil?
          @total_limit_size = @chunk_limit_size * @queue_length_limit
        end
      end

      def start
        super

        @stage, @queue = resume
        @stage.each_pair do |metadata, chunk|
          @metadata_list << metadata unless @metadata_list.include?(metadata)
          @stage_size += chunk.bytesize
        end
        @queue.each do |chunk|
          @metadata_list << chunk.metadata unless @metadata_list.include?(chunk.metadata)
          @queued_num[chunk.metadata] ||= 0
          @queued_num[chunk.metadata] += 1
          @queue_size += chunk.bytesize
        end
      end

      def close
        super
        synchronize do
          @dequeued.each_pair do |chunk_id, chunk|
            chunk.close
          end
          until @queue.empty?
            @queue.shift.close
          end
          @stage.each_pair do |metadata, chunk|
            chunk.close
          end
        end
      end

      def terminate
        super
        @dequeued = @stage = @queue = @queued_num = @metadata_list = nil
        @stage_size = @queue_size = 0
      end

      def storable?
        @total_limit_size > @stage_size + @queue_size
      end

      ## TODO: for back pressure feature
      # def used?(ratio)
      #   @total_size_limit * ratio > @stage_size + @queue_size
      # end

      def resume
        # return {}, []
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

      def new_metadata(timekey: nil, tag: nil, variables: nil)
        Metadata.new(timekey, tag, variables)
      end

      def add_metadata(metadata)
        synchronize do
          if i = @metadata_list.index(metadata)
            @metadata_list[i]
          else
            @metadata_list << metadata
            metadata
          end
        end
      end

      def metadata(timekey: nil, tag: nil, variables: nil)
        meta = new_metadata(timekey: timekey, tag: tag, variables: variables)
        add_metadata(meta)
      end

      # metadata MUST have consistent object_id for each variation
      # data MUST be Array of serialized events
      # metadata_and_data MUST be a hash of { metadata => data }
      def write(metadata_and_data, bulk: false, enqueue: false)
        return if metadata_and_data.size < 1
        raise BufferOverflowError, "buffer space has too many data" unless storable?

        staged_bytesize = 0
        operated_chunks = []

        begin
          metadata_and_data.each do |metadata, data|
            write_once(metadata, data, bulk: bulk) do |chunk, adding_bytesize|
              chunk.mon_enter # add lock to prevent to be committed/rollbacked from other threads
              operated_chunks << chunk
              staged_bytesize += adding_bytesize
            end
          end

          return if operated_chunks.empty?

          first_chunk = operated_chunks.shift
          # Following commits for other chunks also can finish successfully if the first commit operation
          # finishes without any exceptions.
          # In most cases, #commit just requires very small disk spaces, so major failure reason are
          # permission errors, disk failures and other permanent(fatal) errors.
          begin
            first_chunk.commit
            enqueue_chunk(first_chunk.metadata) if enqueue || chunk_size_full?(first_chunk)
            first_chunk.mon_exit
          rescue
            operated_chunks.unshift(first_chunk)
            raise
          end

          errors = []
          # Buffer plugin estimates there's no serious error cause: will commit for all chunks eigher way
          operated_chunks.each do |chunk|
            begin
              chunk.commit
              enqueue_chunk(chunk.metadata) if enqueue || chunk_size_full?(chunk)
              chunk.mon_exit
            rescue => e
              chunk.rollback
              chunk.mon_exit
              errors << e
            end
          end
          operated_chunks.clear if errors.empty?

          @stage_size += staged_bytesize

          if errors.size > 0
            log.warn "error occurs in committing chunks: only first one raised", errors: errors.map(&:class)
            raise errors.first
          end
        ensure
          operated_chunks.each do |chunk|
            chunk.rollback rescue nil # nothing possible to do for #rollback failure
            chunk.mon_exit rescue nil # this may raise ThreadError for chunks already committed
          end
        end
      end

      def queued_records
        synchronize { @queue.reduce(0){|r, chunk| r + chunk.size } }
      end

      def queued?(metadata=nil)
        synchronize do
          if metadata
            n = @queued_num[metadata]
            n && n.nonzero?
          else
            !@queue.empty?
          end
        end
      end

      def enqueue_chunk(metadata)
        synchronize do
          chunk = @stage.delete(metadata)
          return nil unless chunk

          chunk.synchronize do
            if chunk.empty?
              chunk.close
            else
              @queue << chunk
              @queued_num[metadata] = @queued_num.fetch(metadata, 0) + 1
              chunk.enqueued! if chunk.respond_to?(:enqueued!)
            end
          end
          bytesize = chunk.bytesize
          @stage_size -= bytesize
          @queue_size += bytesize
        end
        nil
      end

      def enqueue_all
        synchronize do
          if block_given?
            @stage.keys.each do |metadata|
              chunk = @stage[metadata]
              v = yield metadata, chunk
              enqueue_chunk(metadata) if v
            end
          else
            @stage.keys.each do |metadata|
              enqueue_chunk(metadata)
            end
          end
        end
      end

      def dequeue_chunk
        return nil if @queue.empty?
        synchronize do
          chunk = @queue.shift

          # this buffer is dequeued by other thread just before "synchronize" in this thread
          return nil unless chunk

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
        end
        true
      end

      def purge_chunk(chunk_id)
        synchronize do
          chunk = @dequeued.delete(chunk_id)
          return nil unless chunk # purged by other threads

          metadata = chunk.metadata
          begin
            bytesize = chunk.bytesize
            chunk.purge
            @queue_size -= bytesize
          rescue => e
            log.error "failed to purge buffer chunk", chunk_id: dump_unique_id_hex(chunk_id), error_class: e.class, error: e
          end

          if metadata && !@stage[metadata] && (!@queued_num[metadata] || @queued_num[metadata] < 1)
            @metadata_list.delete(metadata)
          end
        end
        nil
      end

      def clear_queue!
        synchronize do
          until @queue.empty?
            begin
              q = @queue.shift
              log.debug("purging a chunk in queue"){ {id: dump_unique_id_hex(chunk.unique_id), bytesize: chunk.bytesize, size: chunk.size} }
              q.purge
            rescue => e
              log.error "unexpected error while clearing buffer queue", error_class: e.class, error: e
            end
          end
          @queue_size = 0
        end
      end

      def chunk_size_over?(chunk)
        chunk.bytesize > @chunk_limit_size || (@chunk_records_limit && chunk.size > @chunk_records_limit)
      end

      def chunk_size_full?(chunk)
        chunk.bytesize >= @chunk_limit_size * @chunk_full_threshold || (@chunk_records_limit && chunk.size >= @chunk_records_limit * @chunk_full_threshold)
      end

      class ShouldRetry < StandardError; end

      def write_once(metadata, data, bulk: false, &block)
        return if !bulk && (data.nil? || data.empty?)
        return if bulk && (data.empty? || data.first.nil? || data.first.empty?)

        stored = false
        adding_bytesize = nil

        chunk = synchronize { @stage[metadata] ||= generate_chunk(metadata) }
        enqueue_list = []

        chunk.synchronize do
          # retry this method if chunk is already queued (between getting chunk and entering critical section)
          raise ShouldRetry unless chunk.staged?

          empty_chunk = chunk.empty?

          original_bytesize = chunk.bytesize
          begin
            if bulk
              content, size = data
              chunk.concat(content, size)
            else
              chunk.append(data)
            end
            adding_bytesize = chunk.bytesize - original_bytesize

            if chunk_size_over?(chunk)
              if empty_chunk && bulk
                log.warn "chunk bytes limit exceeds for a bulk event stream: #{bulk.bytesize}bytes"
                stored = true
              else
                chunk.rollback
              end
            else
              stored = true
            end
          rescue
            chunk.rollback
            raise
          end

          if stored
            block.call(chunk, adding_bytesize)
          elsif bulk
            # this metadata might be enqueued already by other threads
            # but #enqueue_chunk does nothing in such case
            enqueue_list << metadata
            raise ShouldRetry
          end
        end

        unless stored
          # try step-by-step appending if data can't be stored into existing a chunk in non-bulk mode
          write_step_by_step(metadata, data, data.size / 3, &block)
        end
      rescue ShouldRetry
        enqueue_list.each do |m|
          enqueue_chunk(m)
        end
        retry
      end

      def write_step_by_step(metadata, data, attempt_records, &block)
        while data.size > 0
          if attempt_records < MINIMUM_APPEND_ATTEMPT_RECORDS
            attempt_records = MINIMUM_APPEND_ATTEMPT_RECORDS
          end

          chunk = synchronize{ @stage[metadata] ||= generate_chunk(metadata) }
          chunk.synchronize do # critical section for chunk (chunk append/commit/rollback)
            raise ShouldRetry unless chunk.staged?
            begin
              empty_chunk = chunk.empty?
              original_bytesize = chunk.bytesize

              attempt = data.slice(0, attempt_records)
              chunk.append(attempt)
              adding_bytesize = (chunk.bytesize - original_bytesize)

              if chunk_size_over?(chunk)
                chunk.rollback

                if attempt_records <= MINIMUM_APPEND_ATTEMPT_RECORDS
                  if empty_chunk # record is too large even for empty chunk
                    raise BufferChunkOverflowError, "minimum append butch exceeds chunk bytes limit"
                  end
                  # no more records for this chunk -> enqueue -> to be flushed
                  enqueue_chunk(metadata) # `chunk` will be removed from stage
                  attempt_records = data.size # fresh chunk may have enough space
                else
                  # whole data can be processed by twice operation
                  #  ( by using apttempt /= 2, 3 operations required for odd numbers of data)
                  attempt_records = (attempt_records / 2) + 1
                end

                next
              end

              block.call(chunk, adding_bytesize)
              data.slice!(0, attempt_records)
              # same attempt size
              nil # discard return value of data.slice!() immediately
            rescue
              chunk.rollback
              raise
            end
          end
        end
      rescue ShouldRetry
        retry
      end # write_step_by_step
    end
  end
end
