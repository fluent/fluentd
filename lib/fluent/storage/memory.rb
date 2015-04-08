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

require 'fluent/storage'
require 'fluent/configurable'

module Fluent
  module Storage
    class Memory < Base
      # This is on-memory Hash object itself, to provide same API as plugin storage

      include Configurable

      Fluent::Plugin.register_storage('memory', self)

      config_section :storage, required: false, multi: false, param_name: :storage_config do
        config_set_default :autosave, false
        config_set_default :save_at_shutdown, false
      end

      def initialize
        super
        @store = {}
      end

      def configure(conf)
        super
      end

      def load
        self
      end

      def save
        self
      end

      def put(key, value)
        synchronize do
          @store[key.to_sym] = value
        end
      end

      def get(key)
        synchronize do
          @store[key.to_sym]
        end
      end

      def fetch(key, default_value)
        synchronize do
          @store.fetch(key.to_sym, default_value)
        end
      end

      def delete(key)
        synchronize do
          @store.delete(key.to_sym)
        end
      end
    end
  end
end
