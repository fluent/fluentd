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
    class BareOutput < Base
      include PluginHelper::Mixin # for metrics

      # DO NOT USE THIS plugin for normal output plugin. Use Output instead.
      # This output plugin base class is only for meta-output plugins
      # which cannot be implemented on MultiOutput.
      # E.g,: forest, config-expander

      helpers_internal :metrics

      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin

      def process(tag, es)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def num_errors
        @num_errors_metrics.get
      end

      def emit_count
        @emit_count_metrics.get
      end

      def emit_size
        @emit_size_metrics.get
      end

      def emit_records
        @emit_records_metrics.get
      end

      def initialize
        super
        @counter_mutex = Mutex.new
        # TODO: well organized counters
        @num_errors_metrics = nil
        @emit_count_metrics = nil
        @emit_records_metrics = nil
        @emit_size_metrics = nil
      end

      def configure(conf)
        super

        @num_errors_metrics = metrics_create(namespace: "fluentd", subsystem: "bare_output", name: "num_errors", help_text: "Number of count num errors")
        @emit_count_metrics = metrics_create(namespace: "fluentd", subsystem: "bare_output", name: "emit_records", help_text: "Number of count emits")
        @emit_records_metrics = metrics_create(namespace: "fluentd", subsystem: "bare_output", name: "emit_records", help_text: "Number of emit records")
        @emit_size_metrics =  metrics_create(namespace: "fluentd", subsystem: "bare_output", name: "emit_size", help_text: "Total size of emit events")
        @enable_size_metrics = !!system_config.enable_size_metrics
      end

      def statistics
        stats = {
          'num_errors' => @num_errors_metrics.get,
          'emit_records' => @emit_records_metrics.get,
          'emit_count' => @emit_count_metrics.get,
          'emit_size' => @emit_size_metrics.get,
        }

        { 'bare_output' => stats }
      end

      def emit_sync(tag, es)
        @emit_count_metrics.inc
        begin
          process(tag, es)
          if @enable_size_metrics
            @counter_mutex.synchronize do
              @emit_records_metrics.add(es.size)
              @emit_size_metrics.add(es.to_msgpack_stream.bytesize)
            end
          else
            @emit_records_metrics.add(es.size)
          end
        rescue
          @num_errors_metrics.inc
          raise
        end
      end
      alias :emit_events :emit_sync
    end
  end
end
