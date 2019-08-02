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
require 'fluent/plugin_helper'
require 'fluent/unique_id'
require 'fluent/plugin_id'

module Fluent
  module Plugin
    class ServiceDiscovery < Base
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin
      include UniqueId::Mixin

      configured_in :service_discovery

      attr_reader :services

      Service = Struct.new(:plugin_name, :host, :port, :name, :weight, :standby, :username, :password, :shared_key) do
        def discovery_id
          @discovery_id ||= Base64.encode64(to_h.to_s)
        end
      end

      def initialize
        @services = []

        super
      end
    end
  end
end
