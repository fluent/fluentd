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
module Fluentd
  module Collectors

    require 'fluentd/collector'
    require 'fluentd/event_router'

    class CopyCollector
      include Collector

      def initialize(collectors, opts={})
        @collectors = collectors
        @emit_error_handler = opts[:emit_error_handler] || EventRouter::RaiseEmitErrorHandler.new
      end

      def emit(tag, time, record)
        @collectors.each do |c|
          begin
            c.emit(tag, time, record)
          rescue => e
            @emit_error_handler.handle_emit_error(tag, time, record, e)
          end
        end
      end

      def emits(tag, es)
        @collectors.each do |c|
          begin
            c.emits(tag, es)
          rescue => e
            @emit_error_handler.handle_emits_error(tag, es, e)
          end
        end
      end
    end

  end
end

