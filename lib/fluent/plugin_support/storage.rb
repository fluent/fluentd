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
require 'fluent/plugin'
require 'fluent/plugin_support/timer'
require 'fluent/config/element'

module Fluent
  module PluginSupport
    module Storage
      attr_reader :storage

      include Fluent::PluginSupport::Timer

      def initialize
        super
        @storage = nil
      end

      def configure(conf)
        storage_configs = conf.elements.select{|e| e.name == 'storage' }
        original_conf_elements = conf.elements
        conf.elements = conf.elements.reject{|e| e.name == 'storage' }
        raise Fluent::ConfigError, "<storage> section appears more than once" if storage_configs.size > 1

        super

        # instanciate storage if `<storage>` section exists or <system> plugin_storage_path </system> specified and @id is specified
        if storage_configs.size == 1 || (system_config.plugin_storage_path && plugin_id_configured?)
          storage_config = storage_configs.first
          type = nil
          if storage_config
            type = storage_config.type
          else # /system-wide/specified/plugin-storage-path/json/@id.json
            type = Fluent::Storage::DEFAULT_TYPE
            default_path = File.join(system_config.plugin_storage_path, type, plugin_id + '.' + type)
            storage_config = Fluent::Config::Element.new('storage', '', {'type' => type, 'path' => default_path}, [])
          end

          @storage = Fluent::Plugin.new_storage(type)
          root = Fluent::Config::Element.new('STORAGE_ROOT', '', {}, [storage_config])
          @storage.configure(root)

          # restore original orders of elements
          # to show configuration dump on logs
          conf.elements = original_conf_elements
        else
          @storage = Fluent::Plugin.new_storage('memory')
        end
      end

      def storage_save
        begin
          @storage.save
        rescue => e
          log.error "Failed to save plugin storage contents", storage: @storage.class, error_class: e.class, error: e.message
        end
      end

      def start
        super

        @storage.load

        if @storage.autosave?
          timer_execute(interval: @storage.autosave_interval) do
            storage_save
          end
        end
      end

      def stop
        super
        storage_save if @storage && @storage.save_at_shutdown?
      end

      def shutdown
        super
      end

      def close
        super
        # save storage content once more to be updatable in shutdown sequence
        storage_save if @storage && @storage.save_at_shutdown?
      end

      def terminate
        super
      end
    end
  end
end
