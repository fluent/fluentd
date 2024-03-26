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
require 'fluent/plugin_id'
require 'fluent/plugin_helper'
require 'fluent/unique_id'
require 'fluent/ext_monitor_require'

module Fluent
  module Plugin
    class Buffer < Base
      include OwnedByMixin
      include UniqueId::Mixin
      include PluginId
      include MonitorMixin
      include PluginHelper::Mixin # for metrics

      class BufferError < StandardError; end
      class BufferOverflowError < BufferError; end
      class BufferChunkOverflowError < BufferError; end # A record size is larger than chunk size limit

      MINIMUM_APPEND_ATTEMPT_RECORDS = 10

      DEFAULT_CHUNK_LIMIT_SIZE =   8 * 1024 * 1024 # 8MB
      DEFAULT_TOTAL_LIMIT_SIZE = 512 * 1024 * 1024 # 512MB, same with v0.12 (BufferedOutput + buf_memory: 64 x 8MB)

      DEFAULT_CHUNK_FULL_THRESHOLD = 0.95

      configured_in :buffer

      helpers_internal :metrics

      # TODO: system total buffer limit size in bytes by SystemConfig

      config_param :chunk_limit_size, :size, default: DEFAULT_CHUNK_LIMIT_SIZE
      config_param :total_limit_size, :size, default: DEFAULT_TOTAL_LIMIT_SIZE

      # If user specify this value and (chunk_size * queue_length) is smaller than total_size,
      # then total_size is automatically configured to that value
      config_param :queue_limit_length, :integer, default: nil

      # optional new limitations
      config_param :chunk_limit_records, :integer, default: nil

      # if chunk size (or records) is 95% or more after #write, then that chunk will be enqueued
      config_param :chunk_full_threshold, :float, default: DEFAULT_CHUNK_FULL_THRESHOLD

      desc 'The max number of queued chunks.'
      config_param :queued_chunks_limit_size, :integer, default: nil

      desc 'Compress buffered data.'
      config_param :compress, :enum, list: [:text, :gzip], default: :text

      desc 'If true, chunks are thrown away when unrecoverable error happens'
      config_param :disable_chunk_backup, :bool, default: false

      Metadata = Struct.new(:timekey, :tag, :variables, :seq) do
        def initialize(timekey, tag, variables)
          super(timekey, tag, variables, 0)
        end

        def dup_next
          m = dup
          m.seq = seq + 1
          m
        end

        def empty?
          timekey.nil? && tag.nil? && variables.nil?
        end

        def cmp_variables(v1, v2)
          if v1.nil? && v2.nil?
            return 0
          elsif v1.nil? # v2 is non-nil
            return -1
          elsif v2.nil? # v1 is non-nil
            return 1
          end
          # both of v1 and v2 are non-nil
          v1_sorted_keys = v1.keys.sort
          v2_sorted_keys = v2.keys.sort
          if v1_sorted_keys != v2_sorted_keys
            if v1_sorted_keys.size == v2_sorted_keys.size
              v1_sorted_keys <=> v2_sorted_keys
            else
              v1_sorted_keys.size <=> v2_sorted_keys.size
            end
          else
            v1_sorted_keys.each do |k|
              a = v1[k]
              b = v2[k]
              if a && b && a != b
                return a <=> b
              elsif a && b || (!a && !b) # same value (including both are nil)
                next
              elsif a # b is nil
                return 1
              else # a is nil (but b is non-nil)
                return -1
              end
            end

            0
          end
        end

        def <=>(o)
          timekey2 = o.timekey
          tag2 = o.tag
          variables2 = o.variables
          if (!!timekey ^ !!timekey2) || (!!tag ^ !!tag2) || (!!variables ^ !!variables2)
            # One has value in a field, but another doesn't have value in same field
            # This case occurs very rarely
            if timekey == timekey2 # including the case of nil == nil
              if tag == tag2
                cmp_variables(variables, variables2)
              elsif tag.nil?
                -1
              elsif tag2.nil?
                1
              else
                tag <=> tag2
              end
            elsif timekey.nil?
              -1
            elsif timekey2.nil?
              1
            else
              timekey <=> timekey2
            end
          else
            # objects have values in same field pairs (comparison with non-nil and nil doesn't occur here)
            (timekey <=> timekey2 || 0).nonzero? || # if `a <=> b` is nil, then both are nil
              (tag <=> tag2 || 0).nonzero? ||
              cmp_variables(variables, variables2)
          end
        end

        # This is an optimization code. Current Struct's implementation is comparing all data.
        # https://github.com/ruby/ruby/blob/0623e2b7cc621b1733a760b72af246b06c30cf96/struct.c#L1200-L1203
        # Actually this overhead is very small but this class is generated *per chunk* (and used in hash object).
        # This means that this class is one of the most called object in Fluentd.
        # See https://github.com/fluent/fluentd/pull/2560
        def hash
          timekey.hash
        end
      end

      # for metrics
      attr_reader :stage_size_metrics, :stage_length_metrics, :queue_size_metrics, :queue_length_metrics
      attr_reader :available_buffer_space_ratios_metrics, :total_queued_size_metrics
      attr_reader :newest_timekey_metrics, :oldest_timekey_metrics
      # for tests
      attr_reader :stage, :queue, :dequeued, :queued_num

      def initialize
        super

        @chunk_limit_size = nil
        @total_limit_size = nil
        @queue_limit_length = nil
        @chunk_limit_records = nil

        @stage = {}    #=> Hash (metadata -> chunk) : not flushed yet
        @queue = []    #=> Array (chunks)           : already flushed (not written)
        @dequeued = {} #=> Hash (unique_id -> chunk): already written (not purged)
        @queued_num = {} # metadata => int (number of queued chunks)
        @dequeued_num = {} # metadata => int (number of dequeued chunks)

        @stage_length_metrics = nil
        @stage_size_metrics = nil
        @queue_length_metrics = nil
        @queue_size_metrics = nil
        @available_buffer_space_ratios_metrics = nil
        @total_queued_size_metrics = nil
        @newest_timekey_metrics = nil
        @oldest_timekey_metrics = nil
        @timekeys = Hash.new(0)
        @enable_update_timekeys = false
        @mutex = Mutex.new
      end

      def stage_size
        @stage_size_metrics.get
      end

      def stage_size=(value)
        @stage_size_metrics.set(value)
      end

      def queue_size
        @queue_size_metrics.get
      end

      def queue_size=(value)
        @queue_size_metrics.set(value)
      end

      def persistent?
        false
      end

      def configure(conf)
        super

        unless @queue_limit_length.nil?
          @total_limit_size = @chunk_limit_size * @queue_limit_length
        end
        @stage_length_metrics = metrics_create(namespace: "fluentd", subsystem: "buffer", name: "stage_length",
                                               help_text: 'Length of stage buffers', prefer_gauge: true)
        @stage_length_metrics.set(0)
        @stage_size_metrics = metrics_create(namespace: "fluentd", subsystem: "buffer", name: "stage_byte_size",
                                             help_text: 'Total size of stage buffers', prefer_gauge: true)
        @stage_size_metrics.set(0) # Ensure zero.
        @queue_length_metrics = metrics_create(namespace: "fluentd", subsystem: "buffer", name: "queue_length",
                                               help_text: 'Length of queue buffers', prefer_gauge: true)
        @queue_length_metrics.set(0)
        @queue_size_metrics = metrics_create(namespace: "fluentd", subsystem: "buffer", name: "queue_byte_size",
                                             help_text: 'Total size of queue buffers', prefer_gauge: true)
        @queue_size_metrics.set(0) # Ensure zero.
        @available_buffer_space_ratios_metrics = metrics_create(namespace: "fluentd", subsystem: "buffer", name: "available_buffer_space_ratios",
                                                                help_text: 'Ratio of available space in buffer', prefer_gauge: true)
        @available_buffer_space_ratios_metrics.set(100) # Default is 100%.
        @total_queued_size_metrics = metrics_create(namespace: "fluentd", subsystem: "buffer", name: "total_queued_size",
                                                    help_text: 'Total size of stage and queue buffers', prefer_gauge: true)
        @total_queued_size_metrics.set(0)
        @newest_timekey_metrics = metrics_create(namespace: "fluentd", subsystem: "buffer", name: "newest_timekey",
                                                 help_text: 'Newest timekey in buffer', prefer_gauge: true)
        @oldest_timekey_metrics = metrics_create(namespace: "fluentd", subsystem: "buffer", name: "oldest_timekey",
                                                 help_text: 'Oldest timekey in buffer', prefer_gauge: true)
      end

      def enable_update_timekeys
        @enable_update_timekeys = true
      end

      def start
        super

        @stage, @queue = resume
        @stage.each_pair do |metadata, chunk|
          @stage_size_metrics.add(chunk.bytesize)
        end
        @queue.each do |chunk|
          @queued_num[chunk.metadata] ||= 0
          @queued_num[chunk.metadata] += 1
          @queue_size_metrics.add(chunk.bytesize)
        end
        update_timekeys
        log.debug "buffer started", instance: self.object_id, stage_size: @stage_size_metrics.get, queue_size: @queue_size_metrics.get
      end

      def close
        super
        synchronize do
          log.debug "closing buffer", instance: self.object_id
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
        @dequeued = @stage = @queue = @queued_num = nil
        @stage_length_metrics = @stage_size_metrics = @queue_length_metrics = @queue_size_metrics = nil
        @available_buffer_space_ratios_metrics = @total_queued_size_metrics = nil
        @newest_timekey_metrics = @oldest_timekey_metrics = nil
        @timekeys.clear
      end

      def storable?
        @total_limit_size > @stage_size_metrics.get + @queue_size_metrics.get
      end

      ## TODO: for back pressure feature
      # def used?(ratio)
      #   @total_limit_size * ratio > @stage_size_metrics.get + @queue_size_metrics.get
      # end

      def resume
        # return {}, []
        raise NotImplementedError, "Implement this method in child class"
      end

      def generate_chunk(metadata)
        raise NotImplementedError, "Implement this method in child class"
      end

      def new_metadata(timekey: nil, tag: nil, variables: nil)
        Metadata.new(timekey, tag, variables)
      end

      # Keep this method for existing code
      def metadata(timekey: nil, tag: nil, variables: nil)
        Metadata.new(timekey, tag, variables)
      end

      def timekeys
        @timekeys.keys
      end

      # metadata MUST have consistent object_id for each variation
      # data MUST be Array of serialized events, or EventStream
      # metadata_and_data MUST be a hash of { metadata => data }
      def write(metadata_and_data, format: nil, size: nil, enqueue: false)
        return if metadata_and_data.size < 1
        raise BufferOverflowError, "buffer space has too many data" unless storable?

        log.on_trace { log.trace "writing events into buffer", instance: self.object_id, metadata_size: metadata_and_data.size }

        operated_chunks = []
        unstaged_chunks = {} # metadata => [chunk, chunk, ...]
        chunks_to_enqueue = []
        staged_bytesizes_by_chunk = {}
        # track internal BufferChunkOverflowError in write_step_by_step
        buffer_chunk_overflow_errors = []

        begin
          # sort metadata to get lock of chunks in same order with other threads
          metadata_and_data.keys.sort.each do |metadata|
            data = metadata_and_data[metadata]
            write_once(metadata, data, format: format, size: size) do |chunk, adding_bytesize, error|
              chunk.mon_enter # add lock to prevent to be committed/rollbacked from other threads
              operated_chunks << chunk
              if chunk.staged?
                #
                # https://github.com/fluent/fluentd/issues/2712
                # write_once is supposed to write to a chunk only once
                # but this block **may** run multiple times from write_step_by_step and previous write may be rollbacked
                # So we should be counting the stage_size only for the last successful write
                #
                staged_bytesizes_by_chunk[chunk] = adding_bytesize
              elsif chunk.unstaged?
                unstaged_chunks[metadata] ||= []
                unstaged_chunks[metadata] << chunk
              end
              if error && !error.empty?
                buffer_chunk_overflow_errors << error
              end
            end
          end

          return if operated_chunks.empty?

          # Now, this thread acquires many locks of chunks... getting buffer-global lock causes dead lock.
          # Any operations needs buffer-global lock (including enqueueing) should be done after releasing locks.

          first_chunk = operated_chunks.shift
          # Following commits for other chunks also can finish successfully if the first commit operation
          # finishes without any exceptions.
          # In most cases, #commit just requires very small disk spaces, so major failure reason are
          # permission errors, disk failures and other permanent(fatal) errors.
          begin
            first_chunk.commit
            if enqueue || first_chunk.unstaged? || chunk_size_full?(first_chunk)
              chunks_to_enqueue << first_chunk
            end
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
              if enqueue || chunk.unstaged? || chunk_size_full?(chunk)
                chunks_to_enqueue << chunk
              end
              chunk.mon_exit
            rescue => e
              chunk.rollback
              chunk.mon_exit
              errors << e
            end
          end

          # All locks about chunks are released.

          #
          # Now update the stage, stage_size with proper locking
          # FIX FOR stage_size miscomputation - https://github.com/fluent/fluentd/issues/2712
          #
          staged_bytesizes_by_chunk.each do |chunk, bytesize|
            chunk.synchronize do
              synchronize { @stage_size_metrics.add(bytesize) }
              log.on_trace { log.trace { "chunk #{chunk.path} size_added: #{bytesize} new_size: #{chunk.bytesize}" } }
            end
          end

          chunks_to_enqueue.each do |c|
            if c.staged? && (enqueue || chunk_size_full?(c))
              m = c.metadata
              enqueue_chunk(m)
              if unstaged_chunks[m] && !unstaged_chunks[m].empty?
                u = unstaged_chunks[m].pop
                u.synchronize do
                  if u.unstaged? && !chunk_size_full?(u)
                    # `u.metadata.seq` and `m.seq` can be different but Buffer#enqueue_chunk expect them to be the same value
                    u.metadata.seq = 0
                    synchronize {
                      @stage[m] = u.staged!
                      @stage_size_metrics.add(u.bytesize)
                    }
                  end
                end
              end
            elsif c.unstaged?
              enqueue_unstaged_chunk(c)
            else
              # previously staged chunk is already enqueued, closed or purged.
              # no problem.
            end
          end

          operated_chunks.clear if errors.empty?

          if errors.size > 0
            log.warn "error occurs in committing chunks: only first one raised", errors: errors.map(&:class)
            raise errors.first
          end
        ensure
          operated_chunks.each do |chunk|
            chunk.rollback rescue nil # nothing possible to do for #rollback failure
            if chunk.unstaged?
              chunk.purge rescue nil # to prevent leakage of unstaged chunks
            end
            chunk.mon_exit rescue nil # this may raise ThreadError for chunks already committed
          end
          unless buffer_chunk_overflow_errors.empty?
            # Notify delayed BufferChunkOverflowError here
            raise BufferChunkOverflowError, buffer_chunk_overflow_errors.join(", ")
          end
        end
      end

      def queue_full?
        synchronize { @queue.size } >= @queued_chunks_limit_size
      end

      def queued_records
        synchronize { @queue.reduce(0){|r, chunk| r + chunk.size } }
      end

      def queued?(metadata = nil, optimistic: false)
        if optimistic
          optimistic_queued?(metadata)
        else
          synchronize do
            optimistic_queued?(metadata)
          end
        end
      end

      def enqueue_chunk(metadata)
        log.on_trace { log.trace "enqueueing chunk", instance: self.object_id, metadata: metadata }

        chunk = synchronize do
          @stage.delete(metadata)
        end
        return nil unless chunk

        chunk.synchronize do
          synchronize do
            if chunk.empty?
              chunk.close
            else
              chunk.metadata.seq = 0 # metadata.seq should be 0 for counting @queued_num
              @queue << chunk
              @queued_num[metadata] = @queued_num.fetch(metadata, 0) + 1
              chunk.enqueued!
            end
            bytesize = chunk.bytesize
            @stage_size_metrics.sub(bytesize)
            @queue_size_metrics.add(bytesize)
          end
        end
        nil
      end

      def enqueue_unstaged_chunk(chunk)
        log.on_trace { log.trace "enqueueing unstaged chunk", instance: self.object_id, metadata: chunk.metadata }

        synchronize do
          chunk.synchronize do
            metadata = chunk.metadata
            metadata.seq = 0 # metadata.seq should be 0 for counting @queued_num
            @queue << chunk
            @queued_num[metadata] = @queued_num.fetch(metadata, 0) + 1
            chunk.enqueued!
          end
          @queue_size_metrics.add(chunk.bytesize)
        end
      end

      def update_timekeys
        synchronize do
          chunks = @stage.values
          chunks.concat(@queue)
          @timekeys = chunks.each_with_object({}) do |chunk, keys|
            if chunk.metadata && chunk.metadata.timekey
              t = chunk.metadata.timekey
              keys[t] = keys.fetch(t, 0) + 1
            end
          end
        end
      end

      # At flush_at_shutdown, all staged chunks should be enqueued for buffer flush. Set true to force_enqueue for it.
      def enqueue_all(force_enqueue = false)
        log.on_trace { log.trace "enqueueing all chunks in buffer", instance: self.object_id }
        update_timekeys if @enable_update_timekeys

        if block_given?
          synchronize{ @stage.keys }.each do |metadata|
            return if !force_enqueue && queue_full?
            # NOTE: The following line might cause data race depending on Ruby implementations except CRuby
            # cf. https://github.com/fluent/fluentd/pull/1721#discussion_r146170251
            chunk = @stage[metadata]
            next unless chunk
            v = yield metadata, chunk
            enqueue_chunk(metadata) if v
          end
        else
          synchronize{ @stage.keys }.each do |metadata|
            return if !force_enqueue && queue_full?
            enqueue_chunk(metadata)
          end
        end
      end

      def dequeue_chunk
        return nil if @queue.empty?
        log.on_trace { log.trace "dequeueing a chunk", instance: self.object_id }

        synchronize do
          chunk = @queue.shift

          # this buffer is dequeued by other thread just before "synchronize" in this thread
          return nil unless chunk

          @dequeued[chunk.unique_id] = chunk
          @queued_num[chunk.metadata] -= 1 # BUG if nil, 0 or subzero
          @dequeued_num[chunk.metadata] ||= 0
          @dequeued_num[chunk.metadata] += 1
          log.trace "chunk dequeued", instance: self.object_id, metadata: chunk.metadata
          chunk
        end
      end

      def takeback_chunk(chunk_id)
        log.on_trace { log.trace "taking back a chunk", instance: self.object_id, chunk_id: dump_unique_id_hex(chunk_id) }

        synchronize do
          chunk = @dequeued.delete(chunk_id)
          return false unless chunk # already purged by other thread
          @queue.unshift(chunk)
          log.on_trace { log.trace "chunk taken back", instance: self.object_id, chunk_id: dump_unique_id_hex(chunk_id), metadata: chunk.metadata }
          @queued_num[chunk.metadata] += 1 # BUG if nil
          @dequeued_num[chunk.metadata] -= 1
        end
        true
      end

      def purge_chunk(chunk_id)
        metadata = nil
        synchronize do
          chunk = @dequeued.delete(chunk_id)
          return nil unless chunk # purged by other threads

          metadata = chunk.metadata
          log.on_trace { log.trace "purging a chunk", instance: self.object_id, chunk_id: dump_unique_id_hex(chunk_id), metadata: metadata }

          begin
            bytesize = chunk.bytesize
            chunk.purge
            @queue_size_metrics.sub(bytesize)
          rescue => e
            log.error "failed to purge buffer chunk", chunk_id: dump_unique_id_hex(chunk_id), error_class: e.class, error: e
            log.error_backtrace
          end

          @dequeued_num[chunk.metadata] -= 1
          if metadata && !@stage[metadata] && (!@queued_num[metadata] || @queued_num[metadata] < 1) && @dequeued_num[metadata].zero?
            @queued_num.delete(metadata)
            @dequeued_num.delete(metadata)
          end
          log.on_trace { log.trace "chunk purged", instance: self.object_id, chunk_id: dump_unique_id_hex(chunk_id), metadata: metadata }
        end

        nil
      end

      def clear_queue!
        log.on_trace { log.trace "clearing queue", instance: self.object_id }

        synchronize do
          until @queue.empty?
            begin
              q = @queue.shift
              log.trace("purging a chunk in queue"){ {id: dump_unique_id_hex(chunk.unique_id), bytesize: chunk.bytesize, size: chunk.size} }
              q.purge
            rescue => e
              log.error "unexpected error while clearing buffer queue", error_class: e.class, error: e
              log.error_backtrace
            end
          end
          @queue_size_metrics.set(0)
        end
      end

      def chunk_size_over?(chunk)
        chunk.bytesize > @chunk_limit_size || (@chunk_limit_records && chunk.size > @chunk_limit_records)
      end

      def chunk_size_full?(chunk)
        chunk.bytesize >= @chunk_limit_size * @chunk_full_threshold || (@chunk_limit_records && chunk.size >= @chunk_limit_records * @chunk_full_threshold)
      end

      class ShouldRetry < StandardError; end

      # write once into a chunk
      # 1. append whole data into existing chunk
      # 2. commit it & return unless chunk_size_over?
      # 3. enqueue existing chunk & retry whole method if chunk was not empty
      # 4. go to step_by_step writing

      def write_once(metadata, data, format: nil, size: nil, &block)
        return if data.empty?

        stored = false
        adding_bytesize = nil

        chunk = synchronize { @stage[metadata] ||= generate_chunk(metadata).staged! }
        enqueue_chunk_before_retry = false
        chunk.synchronize do
          # retry this method if chunk is already queued (between getting chunk and entering critical section)
          raise ShouldRetry unless chunk.staged?

          empty_chunk = chunk.empty?

          original_bytesize = chunk.bytesize
          begin
            if format
              serialized = format.call(data)
              chunk.concat(serialized, size ? size.call : data.size)
            else
              chunk.append(data, compress: @compress)
            end
            adding_bytesize = chunk.bytesize - original_bytesize

            if chunk_size_over?(chunk)
              if format && empty_chunk
                if chunk.bytesize > @chunk_limit_size
                  log.warn "chunk bytes limit exceeds for an emitted event stream: #{adding_bytesize}bytes"
                else
                  log.warn "chunk size limit exceeds for an emitted event stream: #{chunk.size}records"
                end
              end
              chunk.rollback

              if format && !empty_chunk
                # Event streams should be appended into a chunk at once
                # as far as possible, to improve performance of formatting.
                # Event stream may be a MessagePackEventStream. We don't want to split it into
                # 2 or more chunks (except for a case that the event stream is larger than chunk limit).
                enqueue_chunk_before_retry = true
                raise ShouldRetry
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
          end
        end

        unless stored
          # try step-by-step appending if data can't be stored into existing a chunk in non-bulk mode
          #
          # 1/10 size of original event stream (splits_count == 10) seems enough small
          # to try emitting events into existing chunk.
          # it does not matter to split event stream into very small splits, because chunks have less
          # overhead to write data many times (even about file buffer chunks).
          write_step_by_step(metadata, data, format, 10, &block)
        end
      rescue ShouldRetry
        enqueue_chunk(metadata) if enqueue_chunk_before_retry
        retry
      end

      # EventStream can be split into many streams
      # because (es1 + es2).to_msgpack_stream == es1.to_msgpack_stream + es2.to_msgpack_stream

      # 1. split event streams into many (10 -> 100 -> 1000 -> ...) chunks
      # 2. append splits into the staged chunks as much as possible
      # 3. create unstaged chunk and append rest splits -> repeat it for all splits

      def write_step_by_step(metadata, data, format, splits_count, &block)
        splits = []
        if splits_count > data.size
          splits_count = data.size
        end
        slice_size = if data.size % splits_count == 0
                       data.size / splits_count
                     else
                       data.size / (splits_count - 1)
                     end
        slice_origin = 0
        while slice_origin < data.size
          splits << data.slice(slice_origin, slice_size)
          slice_origin += slice_size
        end

        # This method will append events into the staged chunk at first.
        # Then, will generate chunks not staged (not queued) to append rest data.
        staged_chunk_used = false
        modified_chunks = []
        modified_metadata = metadata
        get_next_chunk = ->(){
          if staged_chunk_used
            # Staging new chunk here is bad idea:
            # Recovering whole state including newly staged chunks is much harder than current implementation.
            modified_metadata = modified_metadata.dup_next
            generate_chunk(modified_metadata)
          else
            synchronize { @stage[modified_metadata] ||= generate_chunk(modified_metadata).staged! }
          end
        }

        writing_splits_index = 0
        enqueue_chunk_before_retry = false

        while writing_splits_index < splits.size
          chunk = get_next_chunk.call
          errors = []
          # The chunk must be locked until being passed to &block.
          chunk.mon_enter
          modified_chunks << {chunk: chunk, adding_bytesize: 0, errors: errors}

          raise ShouldRetry unless chunk.writable?
          staged_chunk_used = true if chunk.staged?

          original_bytesize = committed_bytesize = chunk.bytesize
          begin
            while writing_splits_index < splits.size
              split = splits[writing_splits_index]
              formatted_split = format ? format.call(split) : nil

              if split.size == 1 # Check BufferChunkOverflowError
                determined_bytesize = nil
                if @compress != :text
                  determined_bytesize = nil
                elsif formatted_split
                  determined_bytesize = formatted_split.bytesize
                elsif split.first.respond_to?(:bytesize)
                  determined_bytesize = split.first.bytesize
                end

                if determined_bytesize && determined_bytesize > @chunk_limit_size
                  # It is a obvious case that BufferChunkOverflowError should be raised here.
                  # But if it raises here, already processed 'split' or
                  # the proceeding 'split' will be lost completely.
                  # So it is a last resort to delay raising such a exception
                  errors << "a #{determined_bytesize} bytes record (nth: #{writing_splits_index}) is larger than buffer chunk limit size (#{@chunk_limit_size})"
                  writing_splits_index += 1
                  next
                end

                if determined_bytesize.nil? || chunk.bytesize + determined_bytesize > @chunk_limit_size
                  # The split will (might) cause size over so keep already processed
                  # 'split' content here (allow performance regression a bit).
                  chunk.commit
                  committed_bytesize = chunk.bytesize
                end
              end

              if format
                chunk.concat(formatted_split, split.size)
              else
                chunk.append(split, compress: @compress)
              end
              adding_bytes = chunk.bytesize - committed_bytesize

              if chunk_size_over?(chunk) # split size is larger than difference between size_full? and size_over?
                chunk.rollback
                committed_bytesize = chunk.bytesize

                if split.size == 1 # Check BufferChunkOverflowError again
                  if adding_bytes > @chunk_limit_size
                    errors << "concatenated/appended a #{adding_bytes} bytes record (nth: #{writing_splits_index}) is larger than buffer chunk limit size (#{@chunk_limit_size})"
                    writing_splits_index += 1
                    next
                  else
                    # As already processed content is kept after rollback, then unstaged chunk should be queued.
                    # After that, re-process current split again.
                    # New chunk should be allocated, to do it, modify @stage and so on.
                    synchronize { @stage.delete(modified_metadata) }
                    staged_chunk_used = false
                    chunk.unstaged!
                    break
                  end
                end

                if chunk_size_full?(chunk) || split.size == 1
                  enqueue_chunk_before_retry = true
                else
                  splits_count *= 10
                end

                raise ShouldRetry
              end

              writing_splits_index += 1

              if chunk_size_full?(chunk)
                break
              end
            end
          rescue
            chunk.purge if chunk.unstaged? # unstaged chunk will leak unless purge it
            raise
          end

          modified_chunks.last[:adding_bytesize] = chunk.bytesize - original_bytesize
        end
        modified_chunks.each do |data|
          block.call(data[:chunk], data[:adding_bytesize], data[:errors])
        end
      rescue ShouldRetry
        modified_chunks.each do |data|
          chunk = data[:chunk]
          chunk.rollback rescue nil
          if chunk.unstaged?
            chunk.purge rescue nil
          end
          chunk.mon_exit rescue nil
        end
        enqueue_chunk(metadata) if enqueue_chunk_before_retry
        retry
      ensure
        modified_chunks.each do |data|
          chunk = data[:chunk]
          chunk.mon_exit
        end
      end

      STATS_KEYS = [
        'stage_length',
        'stage_byte_size',
        'queue_length',
        'queue_byte_size',
        'available_buffer_space_ratios',
        'total_queued_size',
        'oldest_timekey',
        'newest_timekey'
      ]

      def statistics
        stage_size, queue_size = @stage_size_metrics.get, @queue_size_metrics.get
        buffer_space = 1.0 - ((stage_size + queue_size * 1.0) / @total_limit_size)
        @stage_length_metrics.set(@stage.size)
        @queue_length_metrics.set(@queue.size)
        @available_buffer_space_ratios_metrics.set(buffer_space * 100)
        @total_queued_size_metrics.set(stage_size + queue_size)
        stats = {
          'stage_length' => @stage_length_metrics.get,
          'stage_byte_size' => stage_size,
          'queue_length' => @queue_length_metrics.get,
          'queue_byte_size' => queue_size,
          'available_buffer_space_ratios' => @available_buffer_space_ratios_metrics.get.round(1),
          'total_queued_size' => @total_queued_size_metrics.get,
        }

        tkeys = timekeys
        if (m = tkeys.min)
          @oldest_timekey_metrics.set(m)
          stats['oldest_timekey'] = @oldest_timekey_metrics.get
        end
        if (m = tkeys.max)
          @newest_timekey_metrics.set(m)
          stats['newest_timekey'] = @newest_timekey_metrics.get
        end

        { 'buffer' => stats }
      end

      def backup(chunk_unique_id)
        unique_id = dump_unique_id_hex(chunk_unique_id)

        if @disable_chunk_backup
          log.warn "disable_chunk_backup is true. #{unique_id} chunk is not backed up."
          return
        end

        safe_owner_id = owner.plugin_id.gsub(/[ "\/\\:;|*<>?]/, '_')
        backup_base_dir = system_config.root_dir || DEFAULT_BACKUP_DIR
        backup_file = File.join(backup_base_dir, 'backup', "worker#{fluentd_worker_id}", safe_owner_id, "#{unique_id}.log")
        backup_dir = File.dirname(backup_file)

        log.warn "bad chunk is moved to #{backup_file}"
        FileUtils.mkdir_p(backup_dir, mode: system_config.dir_permission || Fluent::DEFAULT_DIR_PERMISSION) unless Dir.exist?(backup_dir)
        File.open(backup_file, 'ab', system_config.file_permission || Fluent::DEFAULT_FILE_PERMISSION) { |f| yield f }
      end

      private

      def optimistic_queued?(metadata = nil)
        if metadata
          n = @queued_num[metadata]
          n && n.nonzero?
        else
          !@queue.empty?
        end
      end
    end
  end
end
