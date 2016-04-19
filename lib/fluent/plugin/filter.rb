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

require 'fluent/plugin/base'

require 'fluent/event'
require 'fluent/log'
require 'fluent/plugin_id'
require 'fluent/plugin_helper'

module Fluent
  module Plugin
    class Filter < Base
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin

      helpers :event_emitter

      def filter(tag, time, record)
        raise NotImplementedError, "BUG: filter plugins MUST implement this method"
      end

      def filter_stream(tag, es)
        new_es = MultiEventStream.new
        es.each do |time, record|
          begin
            filtered_record = filter(tag, time, record)
            new_es.add(time, filtered_record) if filtered_record
          rescue => e
            router.emit_error_event(tag, time, record, e)
          end
        end
        new_es
      end
    end
  end
end
