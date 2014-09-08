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
  class FileBuffer < BasicBuffer
    def self.clear_buffer_paths
      @@buffer_paths = {}
    end
  end

  module Test
    class InputTestDriver < TestDriver
      def initialize(klass, &block)
        FileBuffer.clear_buffer_paths
        super(klass, &block)
        @emit_streams = []
        @expects = nil
        # for checking only the number of emitted records during run
        @expected_emits_length = nil
        @run_timeout = 5
      end

      def expect_emit(tag, time, record)
        (@expects ||= []) << [tag, time, record]
        self
      end

      def expected_emits
        @expects ||= []
      end

      attr_accessor :expected_emits_length
      attr_accessor :run_timeout
      attr_reader :emit_streams

      def emits
        all = []
        @emit_streams.each {|tag,events|
          events.each {|time,record|
            all << [tag, time, record]
          }
        }
        all
      end

      def events
        all = []
        @emit_streams.each {|tag,events|
          all.concat events
        }
        all
      end

      def records
        all = []
        @emit_streams.each {|tag,events|
          events.each {|time,record|
            all << record
          }
        }
        all
      end

      def run(&block)
        m = method(:emit_stream)
        Engine.define_singleton_method(:emit_stream) {|tag,es|
          m.call(tag, es)
        }
        instance.router.define_singleton_method(:emit_stream) {|tag,es|
          m.call(tag, es)
        }
        super {
          block.call if block

          if @expected_emits_length || @expects
            max_length = @expected_emits_length || @expects.length
            started_at = Time.now
            i, j = 0, 0
            while i < max_length && Time.now <= started_at + @run_timeout
              if j >= @emit_streams.length
                sleep 0.01
                next
              end

              tag, events = @emit_streams[j]
              events.each do |time, record|
                assert_equal(@expects[i], [tag, time, record]) if @expects
                i += 1
              end
              j += 1
            end
            assert_equal(@expects.length, i) if @expects
          end
        }
        self
      end

      private
      def emit_stream(tag, es)
        @emit_streams << [tag, es.to_a]
      end
    end
  end
end
