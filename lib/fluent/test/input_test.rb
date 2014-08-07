#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
        @expected_emits_length = nil
        @run_timeout = 5
      end

      attr_accessor :expected_emits_length, :run_timeout
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
        super {
          block.call if block

          if @expected_emits_length
            t0 = Time.now
            i, j = 0, 0
            while i < @expected_emits_length && Time.now <= t0 + @run_timeout
              if j >= @emit_streams.length
                sleep 0.01
                next
              end
              i += @emit_streams[j][1].length
              j += 1
            end
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
