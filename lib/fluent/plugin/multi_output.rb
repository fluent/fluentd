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

        @counters_monitor = Monitor.new
        # TODO: well organized counters
        @num_errors = 0
        @emit_count = 0
        @emit_records = 0
        # @write_count = 0
        # @rollback_count = 0
      end

      def multi_output?
        true
      end

      def configure(conf)
        super

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
            o.send(method_name) unless o.send(checker_name)
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
        @counters_monitor.synchronize{ @emit_count += 1 }
        begin
          process(tag, es)
          @counters_monitor.synchronize{ @emit_records += es.size }
        rescue
          @counters_monitor.synchronize{ @num_errors += 1 }
          raise
        end
      end
      alias :emit_events :emit_sync
    end
  end
end
