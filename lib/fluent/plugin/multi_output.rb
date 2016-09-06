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
        config_param :@type, :string, default: nil
      end

      attr_reader :outputs

      def process(tag, es)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def initialize
        super
        @outputs = []

        @compat = false

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
          if output.has_router?
            output.router = router
          end
          output.configure(store_conf)
          @outputs << output
        end
      end

      # Child plugin's lifecycles are controlled by agent automatically.
      # It calls `outputs` to traverse plugins, and invoke start/stop/*shutdown/close/terminate on these directly.
      # * `start` of this plugin will be called after child plugins
      # * `stop`, `*shutdown`, `close` and `terminate` of this plugin will be called before child plugins

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
