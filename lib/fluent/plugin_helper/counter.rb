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
require 'fluent/counter/client'

module Fluent
  module PluginHelper
    module Counter
      def counter_client_create(scope:, loop: Coolio::Loop.new)
        client_conf = system_config.counter_client
        raise Fluent::ConfigError, '<counter_client> is required in <system>' unless client_conf
        counter_client = Fluent::Counter::Client.new(loop, port: client_conf.port, host: client_conf.host, log: log, timeout: client_conf.timeout)
        counter_client.start
        counter_client.establish(scope)
        @_counter_client = counter_client
        counter_client
      end

      attr_reader :_counter_client

      def initialize
        super
        @_counter_client = nil
      end

      def stop
        super
        @_counter_client.stop
      end

      def terminate
        @_counter_client = nil
        super
      end
    end
  end
end
