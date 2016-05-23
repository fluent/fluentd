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
      # DO NOT USE THIS plugin for normal output plugin. Use Output instead.
      # This output plugin base class is only for meta-output plugins
      # which cannot be implemented on MultiOutput.
      # E.g,: forest, config-expander

      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin

      attr_reader :num_errors, :emit_count, :emit_records

      def process(tag, es)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def initialize
        super
        @counters_monitor = Monitor.new
        # TODO: well organized counters
        @num_errors = 0
        @emit_count = 0
        @emit_records = 0
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
