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
require 'fluent/plugin_helper/retry_state'
require 'fluent/plugin_helper/compat_parameters'

module Fluent
  module PluginHelper
    module Mixin
      def self.included(mod)
        mod.extend(Fluent::PluginHelper)
      end
    end

    def helpers(*snake_case_symbols)
      helper_modules = snake_case_symbols.map{|name| Fluent::PluginHelper.const_get(name.to_s.split('_').map(&:capitalize).join) }
      include(*helper_modules)
    end
  end
end
