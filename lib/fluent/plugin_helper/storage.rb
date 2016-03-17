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

require 'monitor'
require 'forwardable'

require 'fluent/plugin'
require 'fluent/plugin/storage'
require 'fluent/plugin_helper/thread'
require 'fluent/plugin_helper/timer'
require 'fluent/config/element'

module Fluent
  module PluginHelper
    module Storage
      include Fluent::PluginHelper::Thread
      include Fluent::PluginHelper::Timer

      StorageState = Struct.new(:storage, :running)

      def storage_create(usage: '', type: nil, conf: nil)
        s = @_storages[usage]
        if s && s.running
          return s.storage
        elsif !s
          unless type
            raise ArgumentError, "BUG: type not specified without configuration"
          end
          storage = Plugin.new_storage(type)
          config = if conf && conf.is_a?(Fluent::Config::Element)
                     conf
                   elsif conf && conf.is_a?(Hash)
                     conf = Hash[conf.map{|k,v| [k.to_s, v]}]
                     Fluent::Config::Element.new('storage', '', conf, [])
                   else
                     Fluent::Config::Element.new('storage', '', {}, [])
                   end
          storage.configure(config, self)
          s = @_storages[usage] = StorageState.new(wrap_instance(storage), false)
        end

        s.storage.load

        if s.storage.autosave && !s.storage.persistent
          timer_execute(:storage_autosave, s.storage.autosave_interval, repeat: true) do
            begin
              s.storage.save
            rescue => e
              log.error "plugin storage failed to save its data", usage: usage, type: type, error_class: e.class, error: e
            end
          end
        end
        s.running = true
        s.storage
      end

      def self.included(mod)
        mod.instance_eval do
          # minimum section definition to instantiate storage plugin instances
          config_section :storage, required: false, multi: true, param_name: :storage_configs do
            config_argument :usage, :string, default: ''
            config_param    :@type, :string, default: Fluent::Plugin::Storage::DEFAULT_TYPE
          end
        end
      end

      attr_reader :_storages # for tests

      def initialize
        super
        @_storages = {} # usage => storage_state
        @_storages_mutex = Mutex.new
      end

      def configure(conf)
        super

        @storage_configs.each do |section|
          if @_storages[section.usage]
            raise Fluent::ConfigError, "duplicated storages configured: #{section.usage}"
          end
          config = conf.elements.select{|e| e.name == 'storage' && e.arg == section.usage }.first
          raise "storage section with argument '#{section.usage}' not found. it may be a bug." unless config

          storage = Plugin.new_storage(section[:@type])
          storage.configure(config, self)
          @_storages[section.usage] = StorageState.new(wrap_instance(storage), false)
        end
      end

      def stop
        super
        # timer stops automatically
      end

      def shutdown
        @_storages.each_pair do |usage, s|
          begin
            s.storage.save if s.storage.save_at_shutdown
          rescue => e
            log.error "Unexpected error while saving data of plugin storages", usage: usage, storage: s.storage, error_class: e.class, error: e
          end
        end

        super
      end

      def close
        @_storages.each_pair do |usage, s|
          begin
            s.storage.close
          rescue => e
            log.error "Unexpected error while closing plugin storages", usage: usage, storage: s.storage, error_class: e.class, error: e
          end
          s.running = false
        end

        super
      end

      def terminate
        @_storages.each_pair do |usage, s|
          begin
            s.storage.terminate
          rescue => e
            log.error "Unexpected error while terminating plugin storages", usage: usage, storage: s.storage, error_class: e.class, error: e
          end
        end
        @_storages = {}

        super
      end

      def wrap_instance(storage)
        if storage.persistent && storage.persistent_always?
          storage
        elsif storage.persistent
          PersistentWrapper.new(storage)
        elsif !storage.synchronized?
          SynchronizeWrapper.new(storage)
        else
          storage
        end
      end

      class PersistentWrapper
        # PersistentWrapper always provides synchronized operations
        extend Forwardable

        def initialize(storage)
          @storage = storage
          @monitor = Monitor.new
        end

        def_delegators :@storage, :autosave_interval, :save_at_shutdown
        def_delegators :@storage, :close, :terminate

        def persistent_always?
          true
        end

        def persistent
          true
        end

        def autosave
          false
        end

        def synchronized?
          true
        end

        def implementation
          @storage
        end

        def load
          @monitor.synchronize do
            @storage.load
          end
        end

        def save
          @monitor.synchronize do
            @storage.save
          end
        end

        def get(key)
          @monitor.synchronize do
            @storage.load
            @storage.get(key)
          end
        end

        def fetch(key, defval)
          @monitor.synchronize do
            @storage.load
            @storage.fetch(key, defval)
          end
        end

        def put(key, value)
          @monitor.synchronize do
            @storage.load
            @storage.put(key, value)
            @storage.save
            value
          end
        end

        def delete(key)
          @monitor.synchronize do
            @storage.load
            val = @storage.delete(key)
            @storage.save
            val
          end
        end

        def update(key, &block)
          @monitor.synchronize do
            @storage.load
            v = block.call(@storage.get(key))
            @storage.put(key, v)
            @storage.save
            v
          end
        end
      end

      class SynchronizeWrapper
        extend Forwardable

        def initialize(storage)
          @storage = storage
          @mutex = Mutex.new
        end

        def_delegators :@storage, :persistent, :autosave, :autosave_interval, :save_at_shutdown
        def_delegators :@storage, :persistent_always?
        def_delegators :@storage, :close, :terminate

        def synchronized?
          true
        end

        def implementation
          @storage
        end

        def load
          @mutex.synchronize do
            @storage.load
          end
        end

        def save
          @mutex.synchronize do
            @storage.save
          end
        end

        def get(key)
          @mutex.synchronize{ @storage.get(key) }
        end

        def fetch(key, defval)
          @mutex.synchronize{ @storage.fetch(key, defval) }
        end

        def put(key, value)
          @mutex.synchronize{ @storage.put(key, value) }
        end

        def delete(key)
          @mutex.synchronize{ @storage.delete(key) }
        end

        def update(key, &block)
          @mutex.synchronize do
            v = block.call(@storage.get(key))
            @storage.put(key, v)
            v
          end
        end
      end
    end
  end
end
