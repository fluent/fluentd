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


class ObjectSpaceInput < Input
  Plugin.register_input('object_space', self)

  def initialize
    super
  end

  config_param :emit_interval, :time, :default => 60
  config_param :tag, :string
  config_param :top, :integer, :default => 15

  class TimerWatcher < Coolio::TimerWatcher
    def initialize(interval, repeat, &callback)
      @callback = callback
      super(interval, repeat)
    end

    def on_timer
      @callback.call
    rescue
      # TODO log?
      $log.error $!.to_s
      $log.error_backtrace
    end
  end

  def configure(conf)
    super
  end

  def start
    @loop = Coolio::Loop.new
    @timer = TimerWatcher.new(@emit_interval, true, &method(:on_timer))
    @loop.attach(@timer)
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.watchers.each {|w| w.detach }
    @loop.stop
    @thread.join
  end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
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
    now = Engine.now

    array = []
    map = {}

    ObjectSpace.each_object {|obj|
      klass = obj.class
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

    Engine.emit(@tag, now, record)
  end
end


end
