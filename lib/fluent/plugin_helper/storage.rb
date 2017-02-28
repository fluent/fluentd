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

require 'forwardable'

require 'fluent/plugin'
require 'fluent/plugin/storage'
require 'fluent/plugin_helper/timer'
require 'fluent/config/element'
require 'fluent/configurable'

module Fluent
  module PluginHelper
    module Storage
      include Fluent::PluginHelper::Timer

      StorageState = Struct.new(:storage, :running)

      def storage_create(usage: '', type: nil, conf: nil, default_type: nil)
        if conf && conf.respond_to?(:arg) && !conf.arg.empty?
          usage = conf.arg
        end
        if !usage.empty? && usage !~ /^[a-zA-Z][-_.a-zA-Z0-9]*$/
          raise Fluent::ConfigError, "Argument in <storage ARG> uses invalid characters: '#{usage}'"
        end

        s = @_storages[usage]
        if s && s.running
          return s.storage
        elsif s
          # storage is already created, but not loaded / started
        else # !s
          type = if type
                   type
                 elsif conf && conf.respond_to?(:[])
                   raise Fluent::ConfigError, "@type is required in <storage>" unless conf['@type']
                   conf['@type']
                 elsif default_type
                   default_type
                 else
                   raise ArgumentError, "BUG: both type and conf are not specified"
                 end
          storage = Plugin.new_storage(type, parent: self)
          config = case conf
                   when Fluent::Config::Element
                     conf
                   when Hash
                     # in code, programmer may use symbols as keys, but Element needs strings
                     conf = Hash[conf.map{|k,v| [k.to_s, v]}]
                     Fluent::Config::Element.new('storage', usage, conf, [])
                   when nil
                     Fluent::Config::Element.new('storage', usage, {'@type' => type}, [])
                   else
                     raise ArgumentError, "BUG: conf must be a Element, Hash (or unspecified), but '#{conf.class}'"
                   end
          storage.configure(config)
          if @_storages_started
            storage.start
          end
          s = @_storages[usage] = StorageState.new(wrap_instance(storage), false)
        end

        s.storage
      end

      module StorageParams
        include Fluent::Configurable
        # minimum section definition to instantiate storage plugin instances
        config_section :storage, required: false, multi: true, param_name: :storage_configs, init: true do
          config_argument :usage, :string, default: ''
          config_param    :@type, :string, default: Fluent::Plugin::Storage::DEFAULT_TYPE
        end
      end

      def self.included(mod)
        mod.include StorageParams
      end

      attr_reader :_storages # for tests

      def initialize
        super
        @_storages_started = false
        @_storages = {} # usage => storage_state
      end

      def configure(conf)
        super

        @storage_configs.each do |section|
          if !section.usage.empty? && section.usage !~ /^[a-zA-Z][-_.a-zA-Z0-9]*$/
            raise Fluent::ConfigError, "Argument in <storage ARG> uses invalid characters: '#{section.usage}'"
          end
          if @_storages[section.usage]
            raise Fluent::ConfigError, "duplicated storages configured: #{section.usage}"
          end
          storage = Plugin.new_storage(section[:@type], parent: self)
          storage.configure(section.corresponding_config_element)
          @_storages[section.usage] = StorageState.new(wrap_instance(storage), false)
        end
      end

      def start
        super

        @_storages_started = true
        @_storages.each_pair do |usage, s|
          s.storage.start
          s.storage.load

          if s.storage.autosave && !s.storage.persistent
            timer_execute(:storage_autosave, s.storage.autosave_interval, repeat: true) do
              begin
                s.storage.save
              rescue => e
                log.error "plugin storage failed to save its data", usage: usage, type: type, error: e
              end
            end
          end
          s.running = true
        end
      end

      def storage_operate(method_name, &block)
        @_storages.each_pair do |usage, s|
          begin
            block.call(s) if block_given?
            s.storage.send(method_name)
          rescue => e
            log.error "unexpected error while #{method_name}", usage: usage, storage: s.storage, error: e
          end
        end
      end

      def stop
        super
        # timer stops automatically in super
        storage_operate(:stop)
      end

      def before_shutdown
        storage_operate(:before_shutdown)
        super
      end

      def shutdown
        storage_operate(:shutdown) do |s|
          s.storage.save if s.storage.save_at_shutdown
        end
        super
      end

      def after_shutdown
        storage_operate(:after_shutdown)
        super
      end

      def close
        storage_operate(:close){|s| s.running = false }
        super
      end

      def terminate
        storage_operate(:terminate)
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
        def_delegators :@storage, :start, :stop, :before_shutdown, :shutdown, :after_shutdown, :close, :terminate
        def_delegators :@storage, :started?, :stopped?, :before_shutdown?, :shutdown?, :after_shutdown?, :closed?, :terminated?

        def method_missing(name, *args)
          @monitor.synchronize{ @storage.__send__(name, *args) }
        end

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
          @monitor = Monitor.new
        end

        def_delegators :@storage, :persistent, :autosave, :autosave_interval, :save_at_shutdown
        def_delegators :@storage, :persistent_always?
        def_delegators :@storage, :start, :stop, :before_shutdown, :shutdown, :after_shutdown, :close, :terminate
        def_delegators :@storage, :started?, :stopped?, :before_shutdown?, :shutdown?, :after_shutdown?, :closed?, :terminated?

        def method_missing(name, *args)
          @monitor.synchronize{ @storage.__send__(name, *args) }
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
          @monitor.synchronize{ @storage.get(key) }
        end

        def fetch(key, defval)
          @monitor.synchronize{ @storage.fetch(key, defval) }
        end

        def put(key, value)
          @monitor.synchronize{ @storage.put(key, value) }
        end

        def delete(key)
          @monitor.synchronize{ @storage.delete(key) }
        end

        def update(key, &block)
          @monitor.synchronize do
            v = block.call(@storage.get(key))
            @storage.put(key, v)
            v
          end
        end
      end
    end
  end
end
