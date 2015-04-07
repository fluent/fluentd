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

require 'fluent/registry'
require 'fluent/configurable'

module Fluent
  module Storage
    class Base
      include Configurable

      config_section :storage, required: false, multi: false, param_name: :storage_config do
        config_param :type, :string, default: 'json'
        config_param :autosave, :bool, default: true # autosave is done by PluginSupport::Storage in fact
        config_param :autosave_interval, :time, default: 60
        config_param :synchronize, :bool, default: true
      end

      def initialize
        super
        @_storage_mutex = Mutex.new
      end

      def configure(conf)
        super
      end

      def synchronize
        if @synchronize
          @_storage_mutex.synchronize do
            yield
          end
        else
          yield
        end
      end

      def load
        # load storage data from any data source, or initialize storage internally
      end

      def save
        # save internal data store into data source (to be loaded)
      end

      def put(key, value)
        # set of key-valuve store or insert-or-update of RDBMS
        raise ArgumentError, "key must be a string or symbol" unless key.is_a?(String) || key.is_a?(Symbol)
        raise NotImplementedError, "Implement this method in child class"
      end

      def get(key)
        raise ArgumentError, "key must be a string or symbol" unless key.is_a?(String) || key.is_a?(Symbol)
        raise NotImplementedError, "Implement this method in child class"
      end

      def fetch(key, default_value) # just like as Hash#fetch
        raise ArgumentError, "key must be a string or symbol" unless key.is_a?(String) || key.is_a?(Symbol)
        raise NotImplementedError, "Implement this method in child class"
      end
    end
  end
end
