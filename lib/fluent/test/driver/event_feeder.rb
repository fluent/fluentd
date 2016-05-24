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

require 'fluent/event'
require 'fluent/time'

module Fluent
  module Test
    module Driver
      module EventFeeder
        def initialize(klass, opts: {}, &block)
          super
          @default_tag = nil
          @feed_method = nil
        end

        def run(default_tag: nil, **kwargs, &block)
          @feed_method = if @instance.respond_to?(:filter_stream)
                           :filter_stream
                         else
                           :emit_events
                         end
          if default_tag
            @default_tag = default_tag
          end
          super(**kwargs, &block)
        end

        def feed_to_plugin(tag, es)
          @instance.__send__(@feed_method, tag, es)
        end

        # d.run do
        #   d.feed('tag', time, {record})
        #   d.feed('tag', [ [time, {record}], [time, {record}], ... ])
        #   d.feed('tag', es)
        # end
        # d.run(default_tag: 'tag') do
        #   d.feed({record})
        #   d.feed(time, {record})
        #   d.feed([ [time, {record}], [time, {record}], ... ])
        #   d.feed(es)
        # end
        def feed(*args)
          case args.size
          when 1
            raise ArgumentError, "tag not specified without default_tag" unless @default_tag
            case args.first
            when Fluent::EventStream
              feed_to_plugin(@default_tag, args.first)
            when Array
              feed_to_plugin(@default_tag, ArrayEventStream.new(args.first))
            when Hash
              record = args.first
              time = Fluent::EventTime.now
              feed_to_plugin(@default_tag, OneEventStream.new(time, record))
            else
              raise ArgumentError, "unexpected events object (neither event(Hash), EventStream nor Array): #{args.first.class}"
            end
          when 2
            if args[0].is_a?(String) && (args[1].is_a?(Array) || args[1].is_a?(Fluent::EventStream))
              tag, es = args
              es = ArrayEventStream.new(es) if es.is_a?(Array)
              feed_to_plugin(tag, es)
            elsif @default_tag && (args[0].is_a?(Fluent::EventTime) || args[0].is_a?(Integer)) && args[1].is_a?(Hash)
              time, record = args
              feed_to_plugin(@default_tag, OneEventStream.new(time, record))
            else
              raise ArgumentError, "unexpected values of argument: #{args[0].class}, #{args[1].class}"
            end
          when 3
            tag, time, record = args
            if tag.is_a?(String) && (time.is_a?(Fluent::EventTime) || time.is_a?(Integer)) && record.is_a?(Hash)
              feed_to_plugin(tag, OneEventStream.new(time, record))
            else
              raise ArgumentError, "unexpected values of argument: #{tag.class}, #{time.class}, #{record.class}"
            end
          else
            raise ArgumentError, "unexpected number of arguments: #{args}"
          end
        end
      end
    end
  end
end
