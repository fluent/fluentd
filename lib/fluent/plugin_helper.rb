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

require 'fluent/plugin_helper/event_emitter'
require 'fluent/plugin_helper/thread'
require 'fluent/plugin_helper/event_loop'
require 'fluent/plugin_helper/timer'
require 'fluent/plugin_helper/child_process'
require 'fluent/plugin_helper/storage'
require 'fluent/plugin_helper/parser'
require 'fluent/plugin_helper/formatter'
require 'fluent/plugin_helper/inject'
require 'fluent/plugin_helper/extract'
require 'fluent/plugin_helper/socket'
require 'fluent/plugin_helper/server'
require 'fluent/plugin_helper/counter'
require 'fluent/plugin_helper/retry_state'
require 'fluent/plugin_helper/record_accessor'
require 'fluent/plugin_helper/compat_parameters'

module Fluent
  module PluginHelper
    module Mixin
      def self.included(mod)
        mod.extend(Fluent::PluginHelper)
      end
    end

    def self.extended(mod)
      def mod.inherited(subclass)
        subclass.module_eval do
          @_plugin_helpers_list = []
        end
      end
    end

    def helpers_internal(*snake_case_symbols)
      helper_modules = []
      snake_case_symbols.each do |name|
        begin
          helper_modules << Fluent::PluginHelper.const_get(name.to_s.split('_').map(&:capitalize).join)
        rescue NameError
          raise "Unknown plugin helper:#{name}"
        end
      end
      include(*helper_modules)
    end

    def helpers(*snake_case_symbols)
      @_plugin_helpers_list ||= []
      @_plugin_helpers_list.concat(snake_case_symbols)
      helpers_internal(*snake_case_symbols)
    end

    def plugin_helpers
      @_plugin_helpers_list || []
    end
  end
end
