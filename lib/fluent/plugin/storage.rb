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

module Fluent
  module Plugin
    class Storage
      include Fluent::Configurable

      DEFAULT_TYPE = 'json'

      config_param :persistent,        :bool, default: false # load/save with all operations
      config_param :autosave,          :bool, default: true
      config_param :autosave_interval, :time, default: 10
      config_param :save_at_shutdown,  :bool, default: true

      def self.validate_key(key)
        raise ArgumentError, "key must be a string (or symbol for to_s)" unless key.is_a?(String) || key.is_a?(Symbol)
        key.to_s
      end

      def configure(conf, plugin)
        super(conf)

        @_system_config = plugin.system_config
        @_plugin_id = plugin.plugin_id
        @_plugin_id_configured = plugin.plugin_id_configured?
      end

      def persistent_always?
        false
      end

      def synchronized?
        false
      end

      def implementation
        self
      end

      def load
        # load storage data from any data source, or initialize storage internally
      end

      def save
        # save internal data store into data source (to be loaded)
      end

      def get(key)
        raise NotImplementedError, "Implement this method in child class"
      end

      def fetch(key, defval)
        raise NotImplementedError, "Implement this method in child class"
      end

      def put(key, value)
        # return value
        raise NotImplementedError, "Implement this method in child class"
      end

      def delete(key)
        # return deleted value
        raise NotImplementedError, "Implement this method in child class"
      end

      def update(key, &block) # transactional get-and-update
        raise NotImplementedError, "Implement this method in child class"
      end

      # storage plugins has only 'close' and 'terminate'
      # stop: used in helper to stop autosave
      # shutdown: used in helper to call #save finally if needed
      def close; end
      def terminate
        @_system_config = nil
        @_plugin_id = nil
        @_plugin_id_configured = nil
      end
    end
  end
end
