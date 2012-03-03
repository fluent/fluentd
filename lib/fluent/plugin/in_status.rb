
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


class StatusInput < Input
  Plugin.register_input('status', self)

  def initialize
    super
  end

  config_param :emit_interval, :time, :default => 60
  config_param :tag, :string

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

  def on_timer
    now = Engine.now
    Status.each {|record|
      Engine.emit(@tag, now, record)
    }
  end
end


end

