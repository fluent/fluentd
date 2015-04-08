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

require 'fluent/configurable'
require 'fluent/storage'

require 'monitor'

module Fluent
  module Storage
    class Base
      include Configurable

      attr_accessor :autosave, :save_at_shutdown # to be controlled by plugins

      config_section :storage, required: false, multi: false, param_name: :storage_config do
        config_param :type, :string, default: Fluent::Storage::DEFAULT_TYPE
        config_param :save_at_shutdown, :bool, default: true
        config_param :autosave, :bool, default: true # autosave is done by PluginSupport::Storage in fact
        config_param :autosave_interval, :time, default: 60
        config_param :synchronize, :bool, default: true
      end

      def initialize
        super
      end

      def configure(conf)
        super

        @save_at_shutdown = storage_config.save_at_shutdown
        @autosave = storage_config.autosave

        @_storage_monitor = Monitor.new if storage_config.synchronize
      end

      def save_at_shutdown?
        @save_at_shutdown
      end

      def autosave?
        @autosave
      end

      def autosave_interval
        storage_config.autosave_interval
      end

      def synchronize
        if @_storage_monitor
          @_storage_monitor.synchronize do
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

      def delete(key)
        raise ArgumentError, "key must be a string or symbol" unless key.is_a?(String) || key.is_a?(Symbol)
        raise NotImplementedError, "Implement this method in child class"
      end
    end
  end
end
