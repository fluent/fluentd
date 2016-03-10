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
require 'fluent/configurable'
require 'fluent/plugin_id'
require 'fluent/log'
require 'fluent/plugin_helper'

module Fluent
  module Plugin
    class Base
      include Configurable
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin

      def initialize; end
      def configure(conf); end
      def start; end
      def stop; end
      def shutdown; end
      def close; end
      def terminate; end
    end
  end
end
