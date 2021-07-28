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

require 'socket'

require 'fluent/plugin/base'

require 'fluent/log'
require 'fluent/unique_id'
require 'fluent/plugin_id'

module Fluent
  module Plugin
    class Metrics < Base
      include PluginId
      include PluginLoggerMixin
      include UniqueId::Mixin

      DEFAULT_TYPE = 'local'

      configured_in :metrics

      config_param :default_labels, :hash, default: {agent: "Fluentd", hostname: "#{Socket.gethostname}"}
      config_param :labels, :hash, default: {}

      attr_reader :use_gauge_metric
      attr_reader :has_methods_for_gauge, :has_methods_for_counter

      def initialize
        super

        @has_methods_for_counter = false
        @has_methods_for_gauge = false
        @use_gauge_metric = false
      end

      def configure(conf)
        super

        if use_gauge_metric
          @has_methods_for_gauge = has_methods_for_gauge?
        else
          @has_methods_for_counter = has_methods_for_counter?
        end
      end

      # Some metrics should be counted by gauge.
      # ref: https://prometheus.io/docs/concepts/metric_types/#gauge
      def use_gauge_metric=(use_gauge_metric=false)
        @use_gauge_metric = use_gauge_metric
      end

      def create(namespace:, subsystem:,name:,help_text:,labels: {})
        # This API is for cmetrics type.
      end

      def get
        raise NotImplementedError, "Implement this method in child class"
      end

      def inc
        raise NotImplementedError, "Implement this method in child class"
      end

      def dec
        raise NotImplementedError, "Implement this method in child class"
      end

      def add(value)
        raise NotImplementedError, "Implement this method in child class"
      end

      def sub(value)
        raise NotImplementedError, "Implement this method in child class"
      end

      def set(value)
        raise NotImplementedError, "Implement this method in child class"
      end

      private

      def has_methods_for_counter?
        implemented_methods = self.class.instance_methods(false)

        if [:get, :inc, :add].all? {|e| implemented_methods.include?(e)} &&
           [:set].all?{|e| self.class.method_defined?(e)}
          true
        else
          raise "BUG: metrics plugin on counter mode MUST implement `get`, `inc`, `add` methods. And aliased `set` methods should be aliased from another method"
        end
      end

      def has_methods_for_gauge?
        implemented_methods = self.class.instance_methods(false)

        if [:get, :inc, :add].all? {|e| implemented_methods.include?(e)} &&
           [:set, :dec, :sub].all?{|e| self.class.method_defined?(e)}
          true
        else
          raise "BUG: metrics plugin on gauge mode MUST implement `get`, `inc`, and `add` methods. And `dec`, `sub`, and `set` methods should be aliased from other methods"
        end
      end
    end
  end
end
