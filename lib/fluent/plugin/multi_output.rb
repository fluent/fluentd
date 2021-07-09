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
    class MultiOutput < Base
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin # for event_emitter

      helpers :event_emitter # to get router from agent, which will be supplied to child plugins
      helpers_internal :metrics

      config_section :store, param_name: :stores, multi: true, required: true do
        config_argument :arg, :string, default: ''
        config_param :@type, :string, default: nil
      end

      attr_reader :outputs, :outputs_statically_created

      def process(tag, es)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def initialize
        super
        @outputs = []
        @outputs_statically_created = false

        @counter_mutex = Mutex.new
        # TODO: well organized counters
        @num_errors_metrics = nil
        @emit_count_metrics = nil
        @emit_records_metrics = nil
        @emit_size_metrics = nil
        # @write_count = 0
        # @rollback_count = 0
        @enable_size_metrics = false
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

      def statistics
        stats = {
          'num_errors' => @num_errors_metrics.get,
          'emit_records' => @emit_records_metrics.get,
          'emit_count' => @emit_count_metrics.get,
          'emit_size' => @emit_size_metrics.get,
        }

        { 'multi_output' => stats }
      end

      def multi_output?
        true
      end

      def configure(conf)
        super

        @num_errors_metrics = metrics_create(namespace: "fluentd", subsystem: "multi_output", name: "num_errors", help_text: "Number of count num errors")
        @emit_count_metrics = metrics_create(namespace: "fluentd", subsystem: "multi_output", name: "emit_records", help_text: "Number of count emits")
        @emit_records_metrics = metrics_create(namespace: "fluentd", subsystem: "multi_output", name: "emit_records", help_text: "Number of emit records")
        @emit_size_metrics =  metrics_create(namespace: "fluentd", subsystem: "multi_output", name: "emit_size", help_text: "Total size of emit events")
        @enable_size_metrics = !!system_config.enable_size_metrics

        @stores.each do |store|
          store_conf = store.corresponding_config_element
          type = store_conf['@type']
          unless type
            raise Fluent::ConfigError, "Missing '@type' parameter in <store> section"
          end

          log.debug "adding store", type: type

          output = Fluent::Plugin.new_output(type)
          output.context_router = self.context_router
          output.configure(store_conf)
          @outputs << output
        end
      end

      def static_outputs
        @outputs_statically_created = true
        @outputs
      end

      # Child plugin's lifecycles are controlled by agent automatically.
      # It calls `outputs` to traverse plugins, and invoke start/stop/*shutdown/close/terminate on these directly.
      # * `start` of this plugin will be called after child plugins
      # * `stop`, `*shutdown`, `close` and `terminate` of this plugin will be called before child plugins

      # But when MultiOutput plugins are created dynamically (by forest plugin or others), agent cannot find
      # sub-plugins. So child plugins' lifecycles MUST be controlled by MultiOutput plugin itself.
      # TODO: this hack will be removed at v2.
      def call_lifecycle_method(method_name, checker_name)
        return if @outputs_statically_created
        @outputs.each do |o|
          begin
            log.debug "calling #{method_name} on output plugin dynamically created", type: Fluent::Plugin.lookup_type_from_class(o.class), plugin_id: o.plugin_id
            o.__send__(method_name) unless o.__send__(checker_name)
          rescue Exception => e
            log.warn "unexpected error while calling #{method_name} on output plugin dynamically created", plugin: o.class, plugin_id: o.plugin_id, error: e
            log.warn_backtrace
          end
        end
      end

      def start
        super
        call_lifecycle_method(:start, :started?)
      end

      def after_start
        super
        call_lifecycle_method(:after_start, :after_started?)
      end

      def stop
        super
        call_lifecycle_method(:stop, :stopped?)
      end

      def before_shutdown
        super
        call_lifecycle_method(:before_shutdown, :before_shutdown?)
      end

      def shutdown
        super
        call_lifecycle_method(:shutdown, :shutdown?)
      end

      def after_shutdown
        super
        call_lifecycle_method(:after_shutdown, :after_shutdown?)
      end

      def close
        super
        call_lifecycle_method(:close, :closed?)
      end

      def terminate
        super
        call_lifecycle_method(:terminate, :terminated?)
      end

      def emit_sync(tag, es)
        @emit_count_metrics.inc
        begin
          process(tag, es)
          @counter_mutex.synchronize do
            @emit_size_metrics.add(es.to_msgpack_stream.bytesize) if @enable_size_metrics
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
