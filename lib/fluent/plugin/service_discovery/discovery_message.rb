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

require 'fluent/plugin/service_discovery'

module Fluent
  module Plugin
    class ServiceDiscovery # already loaded
      DiscoveryMessage = Struct.new(:type, :service) do
        class << self
          def service_in(service)
            DiscoveryMessage.new(:service_in, service)
          end

          def service_out(service)
            DiscoveryMessage.new(:service_out, service)
          end
        end
      end
    end
  end
end
