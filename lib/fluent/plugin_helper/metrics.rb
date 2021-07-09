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

require 'forwardable'

require 'fluent/plugin'
require 'fluent/plugin/metrics'
require 'fluent/plugin_helper/timer'
require 'fluent/config/element'
require 'fluent/configurable'
require 'fluent/system_config'

module Fluent
  module PluginHelper
    module Metrics
      include Fluent::SystemConfig::Mixin

      attr_reader :_metrics # For tests.

      def initialize
        super
        @_metrics_started = false
        @_metrics = {} # usage => metrics_state
      end

      def configure(conf)
        super

        @plugin_type_or_id = if self.plugin_id_configured?
                               self.plugin_id
                             else
                               "#{conf["@type"] || conf["type"]}.#{self.plugin_id}"
                             end
      end

      def metrics_create(namespace: "fluentd", subsystem: "metrics", name:, help_text:, labels: {}, prefer_gauge: false)
        metrics = if system_config.metrics
                    Fluent::Plugin.new_metrics(system_config.metrics[:@type], parent: self)
                  else
                    Fluent::Plugin.new_metrics(Fluent::Plugin::Metrics::DEFAULT_TYPE, parent: self)
                  end
        config = if system_config.metrics
                   system_config.metrics.corresponding_config_element
                 else
                   Fluent::Config::Element.new('metrics', '', {'@type' => Fluent::Plugin::Metrics::DEFAULT_TYPE}, [])
                 end
        metrics.use_gauge_metric = prefer_gauge
        metrics.configure(config)
        # For multi workers environment, cmetrics should be distinguish with static labels.
        if Fluent::Engine.system_config.workers > 1
          labels.merge!(worker_id: fluentd_worker_id.to_s)
        end
        labels.merge!(plugin: @plugin_type_or_id)
        metrics.create(namespace: namespace, subsystem: subsystem, name: name, help_text: help_text, labels: labels)

        @_metrics["#{@plugin_type_or_id}_#{namespace}_#{subsystem}_#{name}"] = metrics

        metrics
      end

      def metrics_operate(method_name, &block)
        @_metrics.each_pair do |key, m|
          begin
            block.call(s) if block_given?
            m.__send__(method_name)
          rescue => e
            log.error "unexpected error while #{method_name}", key: key, metrics: m, error: e
          end
        end
      end

      def start
        super

        metrics_operate(:start)
        @_metrics_started = true
      end

      def stop
        super
        # timer stops automatically in super
        metrics_operate(:stop)
      end

      def before_shutdown
        metrics_operate(:before_shutdown)
        super
      end

      def shutdown
        metrics_operate(:shutdown)
        super
      end

      def after_shutdown
        metrics_operate(:after_shutdown)
        super
      end

      def close
        metrics_operate(:close)
        super
      end

      def terminate
        metrics_operate(:terminate)
        @_metrics = {}
        super
      end
    end
  end
end
