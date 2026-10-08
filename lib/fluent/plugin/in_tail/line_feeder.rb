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

      # Builds a per-file feed. The flush interval applies only to multiline
      # parsers with a firstline.
      def new_file_feed(flush_interval: nil)
        timer_flush_interval = (@multiline_mode && @parser.has_firstline?) ? flush_interval : nil
        FileFeed.new(self, flush_interval: timer_flush_interval, flush_handler: @flush_handler)
      end

      # Feeds a watcher that uses the deprecated LineBufferTimerFlusher.
      # The flush deadline is started by #parse_multilines for compatibility.
      # @return true if no error or unrecoverable error happens in emit action. false if got BufferOverflowError
      def feed_lines(lines, tail_watcher)
        emit(parse(lines, tail_watcher), tail_watcher)
      end

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

      # Per-file state replacing TailWatcher::LineBufferTimerFlusher.
      class FileFeed
        attr_accessor :line_buffer

        def initialize(line_feeder, flush_interval:, flush_handler:)
          @line_feeder = line_feeder
          @flush_interval = flush_interval
          @flush_handler = flush_handler
          @line_buffer = nil
          @start = nil
        end

        # @return true if no error or unrecoverable error happens in emit action. false if got BufferOverflowError
        def feed_lines(lines, tail_watcher)
          es = @line_feeder.parse(lines, tail_watcher)
          @line_feeder.emit(es, tail_watcher)
        end

        def on_notify(tail_watcher)
          return unless @start

          if Time.now - @start >= @flush_interval
            flush_buffer(@line_buffer, tail_watcher) if @line_buffer
            @line_buffer = nil
            @start = nil
          end
        end

        def close(tail_watcher)
          return unless @line_buffer

          flush_buffer(@line_buffer, tail_watcher)
          @line_buffer = nil
        end

        def flush_buffer(buf, tail_watcher)
          @flush_handler.call(tail_watcher, buf)
        end

        # Starts the flush deadline. Overrides that do not call super may call
        # this method to preserve the old timer behavior.
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

      # Parses multiline input and starts the flush deadline before parsing, as
      # TailInput#parse_multilines did before LineFeeder was extracted.
      def parse_multilines(lines, tail_watcher)
        file_feed = tail_watcher.file_feed
        lb = file_feed.line_buffer
        es = Fluent::MultiEventStream.new
        if @parser.has_firstline?
          file_feed.reset_timer
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
        file_feed.line_buffer = lb
        es
      end

      private

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
