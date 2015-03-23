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
  module Test
    module Driver
      class EventRouter
        def initialize(&block)
          @block = block
          # TODO: emit_error_handler -> logger
        end

        def emit(tag, time, record)
          unless record.nil?
            emit_stream(tag, OneEventStream.new(time, record))
          end
        end

        def emit_array(tag, array)
          emit_stream(tag, ArrayEventStream.new(array))
        end

        def emit_stream(tag, es)
          @block.call(tag, es)
        rescue => e
          @emit_error_handler.handle_emits_error(tag, es, e)
        end
      end
    end
  end
end
