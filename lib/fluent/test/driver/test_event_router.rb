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

module Fluent
  module Test
    module Driver
      class TestEventRouter
        def initialize(driver)
          @driver = driver
        end

        def emit(tag, time, record)
          @driver.emit_event_stream(tag, OneEventStream.new(time, record))
        end

        def emit_array(tag, array)
          @driver.emit_event_stream(tag, ArrayEventStream.new(array))
        end

        def emit_stream(tag, es)
          @driver.emit_event_stream(tag, es)
        end

        def emit_error_event(tag, time, record, error)
          @driver.emit_error_event(tag, time, record, error)
        end
      end
    end
  end
end
