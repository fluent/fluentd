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
require 'fluent/clock'
require 'fluent/file_wrapper'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    class TailWatcher
      class FIFO
        def initialize(encoding, log, max_line_size=nil, encoding_to_convert=nil)
          @buffer = ''.force_encoding(encoding)
          @eol = "\n".encode(encoding).freeze
          @encoding_to_convert = encoding_to_convert
          @max_line_size = max_line_size
          @skip_current_line = false
          @skipping_current_line_bytesize = 0
          @log = log
        end

        attr_reader :buffer, :max_line_size

        def <<(chunk)
          @buffer << chunk
        end

        def convert(s)
          if @encoding_to_convert
            s.encode!(@encoding_to_convert)
          else
            s
          end
        rescue
          s.encode!(@encoding_to_convert, :invalid => :replace, :undef => :replace)
        end

        def read_lines(lines)
          idx = @buffer.index(@eol)
          has_skipped_line = false

          until idx.nil?
            # Using freeze and slice is faster than slice!
            # See https://github.com/fluent/fluentd/pull/2527
            @buffer.freeze
            slice_position = idx + 1
            rbuf = @buffer.slice(0, slice_position)
            @buffer = @buffer.slice(slice_position, @buffer.size - slice_position)
            idx = @buffer.index(@eol)

            is_long_line = @max_line_size && (
              @skip_current_line || rbuf.bytesize > @max_line_size
            )

            if is_long_line
              @log.warn "received line length is longer than #{@max_line_size}"
              if @skip_current_line
                @log.debug("The continuing line is finished. Finally discarded data: ") { convert(rbuf).chomp }
              else
                @log.debug("skipped line: ") { convert(rbuf).chomp }
              end
              has_skipped_line = true
              @skip_current_line = false
              @skipping_current_line_bytesize = 0
              next
            end

            lines << convert(rbuf)
          end

          is_long_current_line = @max_line_size && (
            @skip_current_line || @buffer.bytesize > @max_line_size
          )

          if is_long_current_line
            @log.debug(
              "The continuing current line length is longer than #{@max_line_size}." +
              " The received data will be discarded until this line is finished." +
              " Discarded data: "
            ) { convert(@buffer).chomp }
            @skip_current_line = true
            @skipping_current_line_bytesize += @buffer.bytesize
            @buffer.clear
          end

          return has_skipped_line
        end

        def reading_bytesize
          return @skipping_current_line_bytesize if @skip_current_line
          @buffer.bytesize
        end
      end

      class IOHandler
        BYTES_TO_READ = 64 * 1024
        SHUTDOWN_TIMEOUT = 5

        attr_accessor :shutdown_timeout

        def initialize(watcher, path:, read_lines_limit:, read_bytes_limit_per_second:, max_line_size: nil, log:, open_on_every_update:, encoding: Encoding::ASCII_8BIT, encoding_to_convert: nil, metrics:, &receive_lines)
          @watcher = watcher
          @path = path
          @read_lines_limit = read_lines_limit
          @read_bytes_limit_per_second = read_bytes_limit_per_second
          @receive_lines = receive_lines
          @open_on_every_update = open_on_every_update
          @encoding = encoding
          @fifo = FIFO.new(encoding, log, max_line_size, encoding_to_convert)
          @lines = []
          @io = nil
          @notify_mutex = Mutex.new
          @log = log
          @start_reading_time = nil
          @number_bytes_read = 0
          @shutdown_start_time = nil
          @shutdown_timeout = SHUTDOWN_TIMEOUT
          @shutdown_mutex = Mutex.new
          @eof = false
          @metrics = metrics

          @log.info "following tail of #{@path}"
        end

        def group_watcher
          @watcher.group_watcher
        end

        def on_notify
          @notify_mutex.synchronize { handle_notify }
        end

        def ready_to_shutdown(shutdown_start_time = nil)
          @shutdown_mutex.synchronize {
            @shutdown_start_time =
              shutdown_start_time || Fluent::Clock.now
          }
        end

        def close
          if @io && !@io.closed?
            @io.close
            @io = nil
            @metrics.closed.inc
          end
        end

        def opened?
          !!@io
        end

        def eof?
          @eof
        end

        private

        def limit_bytes_per_second_reached?
          return false if @read_bytes_limit_per_second < 0 # not enabled by conf
          return false if @number_bytes_read < @read_bytes_limit_per_second

          @start_reading_time ||= Fluent::Clock.now
          time_spent_reading = Fluent::Clock.now - @start_reading_time
          @log.debug("time_spent_reading: #{time_spent_reading} #{ @watcher.path}")

          if time_spent_reading < 1
            true
          else
            @start_reading_time = nil
            @number_bytes_read = 0
            false
          end
        end

        def should_shutdown_now?
          # Ensure to read all remaining lines, but abort immediately if it
          # seems to take too long time.
          @shutdown_mutex.synchronize {
            return false if @shutdown_start_time.nil?
            return Fluent::Clock.now - @shutdown_start_time > @shutdown_timeout
          }
        end

        def handle_notify
          if limit_bytes_per_second_reached? || group_watcher&.limit_lines_reached?(@path)
            @metrics.throttled.inc
            return
          end

          with_io do |io|
            iobuf = ''.force_encoding(@encoding)
            begin
              read_more = false
              has_skipped_line = false

              if !io.nil? && @lines.empty?
                begin
                  while true
                    @start_reading_time ||= Fluent::Clock.now
                    group_watcher&.update_reading_time(@path)

                    data = io.readpartial(BYTES_TO_READ, iobuf)
                    @eof = false
                    @number_bytes_read += data.bytesize
                    @fifo << data

                    n_lines_before_read = @lines.size
                    has_skipped_line = @fifo.read_lines(@lines) || has_skipped_line
                    group_watcher&.update_lines_read(@path, @lines.size - n_lines_before_read)

                    group_watcher_limit = group_watcher&.limit_lines_reached?(@path)
                    @log.debug "Reading Limit exceeded #{@path} #{group_watcher.number_lines_read}" if group_watcher_limit

                    if group_watcher_limit || limit_bytes_per_second_reached? || should_shutdown_now?
                      # Just get out from tailing loop.
                      @metrics.throttled.inc if group_watcher_limit || limit_bytes_per_second_reached?
                      read_more = false
                      break
                    end

                    if @lines.size >= @read_lines_limit
                      # not to use too much memory in case the file is very large
                      read_more = true
                      break
                    end
                  end
                rescue EOFError
                  @eof = true
                ensure
                  iobuf.clear
                end
              end

              if @lines.empty?
                @watcher.pe.update_pos(io.pos - @fifo.reading_bytesize) if has_skipped_line
              else
                if @receive_lines.call(@lines, @watcher)
                  @watcher.pe.update_pos(io.pos - @fifo.reading_bytesize)
                  @lines.clear
                else
                  read_more = false
                end
              end
            end while read_more
          end
        end

        def open
          io = Fluent::FileWrapper.open(@path)
          io.seek(@watcher.pe.read_pos + @fifo.reading_bytesize)
          @metrics.opened.inc
          io
        rescue RangeError
          io.close if io
          raise WatcherSetupError, "seek error with #{@path}: file position = #{@watcher.pe.read_pos.to_s(16)}, reading bytesize = #{@fifo.reading_bytesize.to_s(16)}"
        rescue Errno::EACCES => e
          @log.warn "#{e}"
          nil
        rescue Errno::ENOENT
          nil
        end

        def with_io
          if @open_on_every_update
            io = open
            begin
              yield io
            ensure
              io.close unless io.nil?
            end
          else
            @io ||= open
            yield @io
            @eof = true if @io.nil?
          end
        rescue WatcherSetupError => e
          close
          @eof = true
          raise e
        rescue
          @log.error $!.to_s
          @log.error_backtrace
          close
          @eof = true
        end
      end

      class NullIOHandler
        def initialize
        end

        def ready_to_shutdown(shutdown_start_time = nil)
        end

        def io
        end

        def on_notify
        end

        def close
        end

        def opened?
          false
        end

        def eof?
          true
        end
      end
    end
  end
end
