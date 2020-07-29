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

require 'fluent/plugin_helper/cert_option'

module Fluent
  module PluginHelper
    module HttpServer
      # In order not to expose CertOption's methods unnecessary
      class SSLContextBuilder
        include Fluent::PluginHelper::CertOption

        def initialize(log)
          @log = log
        end

        # @param config [Fluent::Config::Section] @transport_config
        def build(config)
          cert_option_create_context(config.version, config.insecure, config.ciphers, config)
        end

        private

        attr_reader :log
      end
    end
  end
end
