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

require 'fluent/plugin/input'
require 'fluent/event'
require 'fluent/plugin/buffer'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    # LineFeeder parses lines read from tailed files into events and emits them.
    # It owns the parser and the options used for parsing and emitting, keeping
    # line processing separate from TailInput's lifecycle.
    # TailInput passes its line processing methods as handlers so that existing
    # TailInput subclasses can continue to override those methods.
    class LineFeeder
      def initialize(parser:, router_provider:, log:, tag:, tag_prefix:, tag_suffix:, path_key:, emit_unmatched_lines:, multiline_mode:, parse_handler: nil, convert_handler: nil, flush_handler: nil)
        @parser = parser
        @router_provider = router_provider
        @log = log
        @tag = tag
        @tag_prefix = tag_prefix
        @tag_suffix = tag_suffix
        @path_key = path_key
        @emit_unmatched_lines = emit_unmatched_lines
        @multiline_mode = multiline_mode
        @parse_handler = parse_handler || if multiline_mode
                                           method(:parse_multilines)
                                         else
                                           method(:parse_singleline)
                                         end
        @convert_handler = convert_handler || method(:convert_line_to_event)
        @flush_handler = flush_handler || method(:flush_buffer)
      end

      # Builds the object which feeds the lines of one tailed file to #parse and
      # #emit, keeping the state of the file: the line buffer of the multiline
      # mode and the time to flush the buffered line.
      #
      # A multiline parser without a firstline keeps buffering the lines until the
      # record is completed, so the flush interval is applied only to a parser
      # with a firstline.
      def new_file_feed(path:, flush_interval: nil)
        timer_flush_interval = (@multiline_mode && @parser.has_firstline?) ? flush_interval : nil
        FileFeed.new(self, path: path, flush_interval: timer_flush_interval, flush_handler: @flush_handler)
      end

      # Feeds the lines read from a watcher which keeps the state of its file in
      # the deprecated TailWatcher::LineBufferTimerFlusher, which a plugin
      # overriding TailInput#setup_watcher may build and pass to TailWatcher by
      # itself. TailInput#receive_lines calls this method for such a watcher,
      # while FileFeed#feed_lines feeds a watcher built by #new_file_feed.
      # @return true if no error or unrecoverable error happens in emit action. false if got BufferOverflowError
      def feed_lines(lines, tail_watcher)
        reset_timer(tail_watcher)
        emit(parse(lines, tail_watcher), tail_watcher)
      end

      # Parses the lines read from the file of the watcher, and returns the events
      # of the complete records.
      #
      # The line which is not a complete record yet is kept in the FileFeed of the
      # watcher, which is built by #new_file_feed.
      def parse(lines, tail_watcher)
        @parse_handler.call(lines, tail_watcher)
      end

      # Emits the events returned by #parse, with the tag of the file of the
      # watcher which the lines were read from.
      # @return true if no error or unrecoverable error happens in emit action. false if got BufferOverflowError
      def emit(es, tail_watcher)
        return true if es.empty?

        begin
          @router_provider.call.emit_stream(tag_for(tail_watcher), es)
        rescue Fluent::Plugin::Buffer::BufferOverflowError
          return false
        rescue
          # ignore non BufferQueueLimitError errors because in_tail can't recover. Engine shows logs and backtraces.
          return true
        end

        return true
      end

      # Flushes the line buffered in the FileFeed of the watcher, which is not a
      # complete record yet, as a record of its own.
      def flush_buffer(tw, buf)
        buf.chomp!
        @parser.parse(buf) { |time, record|
          if time && record
            record[@path_key] ||= tw.path unless @path_key.nil?
            @router_provider.call.emit(tag_for(tw), time, record)
          else
            if @emit_unmatched_lines
              record = { 'unmatched_line' => buf }
              record[@path_key] ||= tw.path unless @path_key.nil?
              @router_provider.call.emit(tag_for(tw), Fluent::EventTime.now, record)
            end
            @log.warn "got incomplete line at shutdown from #{tw.path}: #{buf.inspect}"
          end
        }
      end

      # Feeds the lines read from one tailed file to LineFeeder, keeping the state
      # of the file: the line buffer of the multiline mode and the time when the
      # lines were fed to it.
      #
      # It replaces the deprecated TailWatcher::LineBufferTimerFlusher which
      # TailWatcher kept for each file. The plugins which build it by themselves
      # are fed by LineFeeder#feed_lines instead.
      class FileFeed
        # The line which was read but is not a complete record yet. LineFeeder
        # reads and writes it through the FileFeed of the watcher, so the plugins
        # which override TailInput#parse_multilines can keep the previous
        # signature which takes a TailWatcher.
        attr_accessor :line_buffer

        def initialize(line_feeder, path:, flush_interval:, flush_handler:)
          @line_feeder = line_feeder
          @path = path
          @flush_interval = flush_interval
          @flush_handler = flush_handler
          @line_buffer = nil
          @start = nil
        end

        attr_reader :path

        # Parses the lines read from the file of the watcher and emits the events
        # of the complete records, keeping the line which is not a complete record
        # yet until the next line of the file is read.
        #
        # The deadline to flush the buffered line starts before the lines are
        # parsed, like TailWatcher::LineBufferTimerFlusher did, so a slow parser
        # does not postpone it.
        #
        # @return true if no error or unrecoverable error happens in emit action. false if got BufferOverflowError
        def feed_lines(lines, tail_watcher)
          reset_timer
          es = @line_feeder.parse(lines, tail_watcher)
          @line_feeder.emit(es, tail_watcher)
        end

        # Flushes the buffered line when it has not been completed for the flush
        # interval, like LineBufferTimerFlusher#on_notify did.
        def on_notify(tail_watcher)
          return unless @start

          if Time.now - @start >= @flush_interval
            flush_buffer(@line_buffer, tail_watcher) if @line_buffer
            @line_buffer = nil
            @start = nil
          end
        end

        # Flushes the buffered line and forgets it, like LineBufferTimerFlusher#close did.
        def close(tail_watcher)
          return unless @line_buffer

          flush_buffer(@line_buffer, tail_watcher)
          @line_buffer = nil
        end

        # Flushes the given line of the file as a record of its own, calling the
        # flush handler of LineFeeder to keep TailInput#flush_buffer of the
        # subclasses of TailInput on the call path.
        def flush_buffer(buf, tail_watcher)
          @flush_handler.call(tail_watcher, buf)
        end

        private

        def reset_timer
          return unless @flush_interval

          @start = Time.now
        end
      end

      def convert_line_to_event(line, es, tail_watcher)
        begin
          line.chomp!  # remove \n
          @parser.parse(line) { |time, record|
            if time && record
              record[@path_key] ||= tail_watcher.path unless @path_key.nil?
              es.add(time, record)
            else
              if @emit_unmatched_lines
                record = {'unmatched_line' => line}
                record[@path_key] ||= tail_watcher.path unless @path_key.nil?
                es.add(Fluent::EventTime.now, record)
              end
              @log.warn { "pattern not matched: #{line.inspect}" }
            end
          }
        rescue => e
          @log.warn 'invalid line found', file: tail_watcher.path, line: line, error: e.to_s
          @log.debug_backtrace(e.backtrace)
        end
      end

      def parse_singleline(lines, tail_watcher)
        es = Fluent::MultiEventStream.new
        lines.each { |line|
          @convert_handler.call(line, es, tail_watcher)
        }
        es
      end

      def parse_multilines(lines, tail_watcher)
        file_feed = tail_watcher.file_feed
        lb = file_feed.line_buffer
        es = Fluent::MultiEventStream.new
        if @parser.has_firstline?
          lines.each { |line|
            if @parser.firstline?(line)
              if lb
                @convert_handler.call(lb, es, tail_watcher)
              end
              lb = line
            else
              if lb.nil?
                if @emit_unmatched_lines
                  @convert_handler.call(line, es, tail_watcher)
                end
                @log.warn "got incomplete line before first line from #{tail_watcher.path}: #{line.inspect}"
              else
                lb << line
              end
            end
          }
        else
          lb ||= ''
          lines.each do |line|
            lb << line
            @parser.parse(lb) { |time, record|
              if time && record
                @convert_handler.call(lb, es, tail_watcher)
                lb = ''
              end
            }
          end
        end
        tail_watcher.file_feed.line_buffer = lb
        es
      end

      private

      # Starts the deadline of the line buffered by the deprecated
      # TailWatcher::LineBufferTimerFlusher at the beginning of the parsing, like
      # TailInput#parse_multilines did before LineFeeder::FileFeed replaced it.
      # A multiline parser without a firstline keeps buffering the lines until the
      # record is completed, so the deadline is not started for it.
      def reset_timer(tail_watcher)
        return unless @multiline_mode && @parser.has_firstline?

        tail_watcher.line_buffer_timer_flusher&.reset_timer
      end

      def tag_for(tw)
        if @tag_prefix || @tag_suffix
          @tag_prefix + tw.tag + @tag_suffix
        else
          @tag
        end
      end
    end
  end
end
