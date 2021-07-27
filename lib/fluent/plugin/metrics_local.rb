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

require 'fluent/plugin'
require 'fluent/plugin/metrics'

module Fluent
  module Plugin
    class LocalMetrics < Metrics
      Fluent::Plugin.register_metrics('local', self)

      def initialize
        super
        @store = Hash.new(0)
        @monitor = Monitor.new
      end

      def configure(conf)
        super

        if use_gauge_metric
          class << self
            alias_method :dec, :dec_gauge
            alias_method :set, :set_gauge
            alias_method :sub, :sub_gauge
          end
        else
          class << self
            alias_method :set, :set_counter
          end
        end
      end

      def multi_workers_ready?
        true
      end

      def get(key)
        @monitor.synchronize do
          @store[key.to_s]
        end
      end

      def inc(key)
        @monitor.synchronize do
          @store[key.to_s] += 1
        end
      end

      def dec_gauge(key)
        @monitor.synchronize do
          @store[key.to_s] -= 1
        end
      end

      def add(key, value)
        @monitor.synchronize do
          @store[key.to_s] += value
        end
      end

      def sub_gauge(key, value)
        @monitor.synchronize do
          @store[key.to_s] -= value
        end
      end

      def set_counter(key, value)
        return if @store[key.to_s] > value

        @monitor.synchronize do
          @store[key.to_s] = value
        end
      end

      def set_gauge(key, value)
        @monitor.synchronize do
          @store[key.to_s] = value
        end
      end
    end
  end
end
