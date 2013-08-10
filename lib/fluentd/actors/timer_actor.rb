#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
module Fluentd
  module Actors

    module TimerActor
      def every(interval_sec, &callback)
        @loop.attach CallbackTimerWatcher.new(interval_sec, true, &callback)
      end

      def after(sec, &callback)
        @loop.attach CallbackTimerWatcher.new(sec, false, &callback)
      end

      class CallbackTimerWatcher < Coolio::TimerWatcher
        def initialize(interval, repeat, &callback)
          @callback = callback
          super(interval, repeat)
        end

        def on_timer
          @callback.call
        rescue
          Fluentd.log.error $!.to_s
          Fluentd.log.error_backtrace
        end
      end
    end

  end
end
