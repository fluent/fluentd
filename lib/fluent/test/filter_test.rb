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

require 'fluent/test/base'
require 'fluent/event'

module Fluent
  module Test
    class FilterTestDriver < TestDriver
      def initialize(klass, tag = 'filter.test', &block)
        super(klass, &block)
        @tag = tag
        @events = {}
        @filtered = MultiEventStream.new
      end

      attr_reader :filtered
      attr_accessor :tag

      def emit(record, time = Engine.now)
        emit_with_tag(@tag, record, time)
      end
      alias_method :filter, :emit

      def emit_with_tag(tag, record, time = Engine.now)
        @events[tag] ||= MultiEventStream.new
        @events[tag].add(time, record)
      end
      alias_method :filter_with_tag, :emit_with_tag

      def filter_stream(es)
        filter_stream_with_tag(@tag, es)
      end

      def filter_stream_with_tag(tag, es)
        @events[tag] = es
      end

      def filtered_as_array
        all = []
        @filtered.each { |time, record|
          all << [@tag, time, record]
        }
        all
      end
      alias_method :emits, :filtered_as_array # emits is for consistent with other drivers

      # Almost filters don't use threads so default is 0. It reduces test time.
      def run(num_waits = 0, &block)
        super(num_waits) {
          block.call if block

          @events.each { |tag, es|
            processed = @instance.filter_stream(tag, es)
            processed.each { |time, record|
              @filtered.add(time, record)
            }
          }
        }
        self
      end
    end
  end
end
