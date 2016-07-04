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

require 'cool.io'
require 'yajl'

require 'fluent/plugin/input'

module Fluent::Plugin
  class ObjectSpaceInput < Fluent::Plugin::Input
    Fluent::Plugin.register_input('object_space', self)

    helpers :event_loop

    def initialize
      super
    end

    config_param :emit_interval, :time, default: 60
    config_param :tag, :string
    config_param :top, :integer, default: 15

    class TimerWatcher < ::Coolio::TimerWatcher
      def initialize(interval, repeat, log, &callback)
        @callback = callback
        @log = log
        super(interval, repeat)
      end

      def on_timer
        @callback.call
      rescue
        # TODO log?
        @log.error $!.to_s
        @log.error_backtrace
      end
    end

    def configure(conf)
      super
    end

    def start
      super

      @timer = TimerWatcher.new(@emit_interval, true, log, &method(:on_timer))
      event_loop_attach(@timer)
    end

    def shutdown
      super
    end

    class Counter
      def initialize(klass, init_count)
        @klass = klass
        @count = init_count
      end

      def incr!
        @count += 1
      end

      def name
        @klass.name
      end

      attr_reader :count
    end

    def on_timer
      now = Fluent::EventTime.now

      array = []
      map = {}

      ObjectSpace.each_object {|obj|
        klass = obj.class rescue Object
        if c = map[klass]
          c.incr!
        else
          c = Counter.new(klass, 1)
          array << c
          map[klass] = c
        end
      }

      array.sort_by! {|c| -c.count }

      record = {}
      array.each_with_index {|c,i|
        break if i >= @top
        record[c.name] = c.count
      }

      router.emit(@tag, now, record)
    rescue => e
      log.error "object space failed to emit", error: e, tag: @tag, record: Yajl.dump(record)
      log.error_backtrace
    end
  end
end
