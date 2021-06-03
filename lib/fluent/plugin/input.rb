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

require 'fluent/log'
require 'fluent/plugin_id'
require 'fluent/plugin_helper'

module Fluent
  module Plugin
    class Input < Base
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin

      helpers_internal :event_emitter

      def initialize
        super
        @emit_records = 0
        @emit_size = 0
        @counter_mutex = Mutex.new
      end

      def statistics
        stats = {
          'emit_records' => @emit_records,
          'emit_size' => @emit_size,
        }

        { 'input' => stats }
      end

      def metric_callback(es)
        @counter_mutex.synchronize { @emit_records += 1 }
        @counter_mutex.synchronize { @emit_size += es.to_msgpack_stream.bytesize }
      end

      def multi_workers_ready?
        false
      end
    end
  end
end
