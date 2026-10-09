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

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    module Compatibility
      # Keep these methods as compatibility entry points for plugins which
      # inherit TailInput and override line processing methods. TailInput is a
      # built-in plugin, not a public inheritance API, and these methods may be
      # removed in a future release.
      def flush_buffer(tw, buf)
        @line_feeder.flush_buffer(tw, buf)
      end

      # Feeds lines through the per-file feed, or through LineFeeder for watchers
      # created by plugins using the deprecated LineBufferTimerFlusher.
      #
      # @return true if no error or unrecoverable error happens in emit action. false if got BufferOverflowError
      def receive_lines(lines, tail_watcher)
        file_feed = tail_watcher.file_feed
        return file_feed.feed_lines(lines, tail_watcher) if file_feed.respond_to?(:feed_lines)

        @line_feeder.feed_lines(lines, tail_watcher)
      end

      def convert_line_to_event(line, es, tail_watcher)
        @line_feeder.convert_line_to_event(line, es, tail_watcher)
      end

      def parse_singleline(lines, tail_watcher)
        @line_feeder.parse_singleline(lines, tail_watcher)
      end

      # TailInput's default #parse_multilines path starts the flush timer before
      # parsing. An override that skips super must call
      # tail_watcher.line_buffer_timer_flusher.reset_timer when needed.
      def parse_multilines(lines, tail_watcher)
        @line_feeder.parse_multilines(lines, tail_watcher)
      end
    end

    class TailWatcher
      # The object which keeps the line buffer of the multiline mode of the file:
      # the FileFeed built by LineFeeder#new_file_feed, or the deprecated
      # LineBufferTimerFlusher which a plugin overriding TailInput#setup_watcher
      # passes to keep working without changes.
      def line_buffer_timer_flusher
        @file_feed
      end

      # Kept for compatibility with the plugins which build it by themselves and
      # pass it to TailWatcher in the overridden TailInput#setup_watcher.
      # LineFeeder#feed_lines feeds the lines of a watcher which keeps it, and
      # LineFeeder::FileFeed built by LineFeeder#new_file_feed replaces it.
      class LineBufferTimerFlusher
        attr_accessor :line_buffer

        def initialize(log, flush_interval, &flush_method)
          @log = log
          @flush_interval = flush_interval
          @flush_method = flush_method
          @start = nil
          @line_buffer = nil
        end

        def on_notify(tw)
          unless @start && @flush_method
            return
          end

          if Time.now - @start >= @flush_interval
            @flush_method.call(tw, @line_buffer) if @line_buffer
            @line_buffer = nil
            @start = nil
          end
        end

        def close(tw)
          return unless @line_buffer

          @flush_method.call(tw, @line_buffer)
          @line_buffer = nil
        end

        def reset_timer
          return unless @flush_interval

          @start = Time.now
        end
      end
    end
  end
end
