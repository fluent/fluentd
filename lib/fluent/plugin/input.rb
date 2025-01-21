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

      helpers_internal :event_emitter, :metrics

      def initialize
        super
        @emit_records_metrics = nil
        @emit_size_metrics = nil
        @counter_mutex = Mutex.new
        @enable_size_metrics = false
      end

      def emit_records
        @emit_records_metrics.get
      end

      def emit_size
        @emit_size_metrics.get
      end

      def configure(conf)
        super

        @emit_records_metrics = metrics_create(namespace: "fluentd", subsystem: "input", name: "emit_records", help_text: "Number of count emit records")
        @emit_size_metrics = metrics_create(namespace: "fluentd", subsystem: "input", name: "emit_size", help_text: "Total size of emit events")
        @enable_size_metrics = !!system_config.enable_size_metrics
      end

      def statistics
        stats = {
          'emit_records' => @emit_records_metrics.get,
          'emit_size' => @emit_size_metrics.get,
        }

        { 'input' => stats }
      end

      def metric_callback(es)
        @emit_records_metrics.add(es.size)
        @emit_size_metrics.add(es.to_msgpack_stream.bytesize) if @enable_size_metrics
      end

      def multi_workers_ready?
        false
      end

      def zero_downtime_restart_ready?
        false
      end
    end
  end
end
