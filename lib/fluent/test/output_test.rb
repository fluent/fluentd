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

      def emit(record, time=Time.now)
        es = OneEventStream.new(time.to_i, record)
        chain = TestOutputChain.new
        @instance.emit(@tag, es, chain)
        assert_equal 1, chain.called
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

      def emit(record, time=Time.now)
        @entries << [time.to_i, record]
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

          case
          when @instance.is_a?(Fluent::ObjectBufferedOutput)
            key = @tag
          when @instance.respond_to?(:time_slicer)
            # this block is only for test_out_file
            time, _record = @entries.first
            key = @instance.time_slicer.call(time)
          else
            key = ''
          end
          chunk = @instance.buffer.new_chunk(key)
          chunk << buffer
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
        @entries = {}
        @expected_buffer = nil
        @tag = tag
      end

      attr_accessor :tag

      def emit(record, time=Time.now)
        slicer = @instance.instance_eval{@time_slicer}
        key = slicer.call(time.to_i)
        @entries[key] = [] unless @entries.has_key?(key)
        @entries[key] << [time.to_i, record]
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
          @entries.keys.each {|key|
            es = ArrayEventStream.new(@entries[key])
            @instance.emit(@tag, es, NullOutputChain.instance)
            buffer << @instance.format_stream(@tag, es)
          }

          if @expected_buffer
            assert_equal(@expected_buffer, buffer)
          end

          chunks = @instance.instance_eval {
            @buffer.instance_eval {
              chunks = []
              @map.keys.each {|key|
                chunks.push(@map.delete(key))
              }
              chunks
            }
          }
          chunks.each { |chunk|
            result.push(@instance.write(chunk))
          }
        }
        result
      end
    end
  end
end
