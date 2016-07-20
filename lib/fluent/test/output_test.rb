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

require 'fluent/engine'
require 'fluent/event'
require 'fluent/test/input_test'

module Fluent
  module Test
    class TestOutputChain
      def initialize
        @called = 0
      end

      def next
        @called += 1
      end

      attr_reader :called
    end


    class OutputTestDriver < InputTestDriver
      def initialize(klass, tag='test', &block)
        super(klass, &block)
        @tag = tag
      end

      attr_accessor :tag

      def emit(record, time=Engine.now)
        es = OneEventStream.new(time, record)
        @instance.emit_events(@tag, es)
      end
    end


    class BufferedOutputTestDriver < InputTestDriver
      def initialize(klass, tag='test', &block)
        super(klass, &block)
        @entries = []
        @expected_buffer = nil
        @tag = tag

        def @instance.buffer
          @buffer
        end
      end

      attr_accessor :tag

      def emit(record, time=Engine.now)
        @entries << [time, record]
        self
      end

      def expect_format(str)
        (@expected_buffer ||= '') << str
      end

      def run(num_waits = 10, &block)
        result = nil
        super(num_waits) {
          block.call if block

          es = ArrayEventStream.new(@entries)
          buffer = @instance.format_stream(@tag, es)

          if @expected_buffer
            assert_equal(@expected_buffer, buffer)
          end

          chunk = if @instance.instance_eval{ @chunk_key_tag }
                    @instance.buffer.generate_chunk(@instance.metadata(@tag, nil, nil)).staged!
                  else
                    @instance.buffer.generate_chunk(@instance.metadata(nil, nil, nil)).staged!
                  end
          chunk.concat(buffer, es.size)

          begin
            result = @instance.write(chunk)
          ensure
            chunk.purge
          end
        }
        result
      end
    end

    class TimeSlicedOutputTestDriver < InputTestDriver
      def initialize(klass, tag='test', &block)
        super(klass, &block)
        @entries = []
        @expected_buffer = nil
        @tag = tag
      end

      attr_accessor :tag

      def emit(record, time=Engine.now)
        @entries << [time, record]
        self
      end

      def expect_format(str)
        (@expected_buffer ||= '') << str
      end

      def run(&block)
        result = []
        super {
          block.call if block

          buffer = ''
          lines = {}
          # v0.12 TimeSlicedOutput doesn't call #format_stream
          @entries.each do |time, record|
            meta = @instance.metadata(@tag, time, record)
            line = @instance.format(@tag, time, record)
            buffer << line
            lines[meta] ||= []
            lines[meta] << line
          end

          if @expected_buffer
            assert_equal(@expected_buffer, buffer)
          end

          lines.keys.each do |meta|
            chunk = @instance.buffer.generate_chunk(meta).staged!
            chunk.append(lines[meta])
            begin
              result.push(@instance.write(chunk))
            ensure
              chunk.purge
            end
          end
        }
        result
      end
    end
  end
end
