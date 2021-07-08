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

      helpers_internal :event_emitter, :metrics

      attr_reader :has_filter_with_time

      def initialize
        super
        @has_filter_with_time = has_filter_with_time?
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

        @emit_records_metrics = metrics_create(namespace: "Fluentd", subsystem: "filter", name: "emit_records", help_text: "Number of count emit records")
        @emit_size_metrics = metrics_create(namespace: "Fluentd", subsystem: "filter", name: "emit_size", help_text: "Total size of emit events")
        @enable_size_metrics = !!system_config.enable_size_metrics
      end

      def statistics
        stats = {
          'emit_records' => @emit_records_metrics.get,
          'emit_size' => @emit_size_metrics.get,
        }

        { 'filter' => stats }
      end

      def measure_metrics(es)
        @counter_mutex.synchronize do
          @emit_records_metrics.add(es.size)
          @emit_size_metrics.add(es.to_msgpack_stream.bytesize) if @enable_size_metrics
        end
      end

      def filter(tag, time, record)
        raise NotImplementedError, "BUG: filter plugins MUST implement this method"
      end

      def filter_with_time(tag, time, record)
        raise NotImplementedError, "BUG: filter plugins MUST implement this method"
      end

      def filter_stream(tag, es)
        new_es = MultiEventStream.new
        if @has_filter_with_time
          es.each do |time, record|
            begin
              filtered_time, filtered_record = filter_with_time(tag, time, record)
              new_es.add(filtered_time, filtered_record) if filtered_time && filtered_record
            rescue => e
              router.emit_error_event(tag, time, record, e)
            end
          end
        else
          es.each do |time, record|
            begin
              filtered_record = filter(tag, time, record)
              new_es.add(time, filtered_record) if filtered_record
            rescue => e
              router.emit_error_event(tag, time, record, e)
            end
          end
        end
        new_es
      end

      private

      def has_filter_with_time?
        implmented_methods = self.class.instance_methods(false)
        # Plugins that override `filter_stream` don't need check,
        # because they may not call `filter` or `filter_with_time`
        # for example fluentd/lib/fluent/plugin/filter_record_transformer.rb
        return nil if implmented_methods.include?(:filter_stream)
        case
        when [:filter, :filter_with_time].all? { |e| implmented_methods.include?(e) }
          raise "BUG: Filter plugins MUST implement either `filter` or `filter_with_time`"
        when implmented_methods.include?(:filter)
          false
        when implmented_methods.include?(:filter_with_time)
          true
        else
          raise NotImplementedError, "BUG: Filter plugins MUST implement either `filter` or `filter_with_time`"
        end
      end
    end
  end
end
