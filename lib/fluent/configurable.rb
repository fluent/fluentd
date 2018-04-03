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

require 'fluent/config/configure_proxy'
require 'fluent/config/section'
require 'fluent/config/error'
require 'fluent/registry'
require 'fluent/plugin'
require 'fluent/config/types'

module Fluent
  module Configurable
    def self.included(mod)
      mod.extend(ClassMethods)
    end

    def initialize
      super
      # to simulate implicit 'attr_accessor' by config_param / config_section and its value by config_set_default
      proxy = self.class.merged_configure_proxy
      proxy.params.keys.each do |name|
        next if name.to_s.start_with?('@')
        if proxy.defaults.has_key?(name)
          instance_variable_set("@#{name}".to_sym, proxy.defaults[name])
        end
      end
      proxy.sections.keys.each do |name|
        next if name.to_s.start_with?('@')
        subproxy = proxy.sections[name]
        if subproxy.multi?
          instance_variable_set("@#{subproxy.variable_name}".to_sym, [])
        else
          instance_variable_set("@#{subproxy.variable_name}".to_sym, nil)
        end
      end
    end

    def configure_proxy_generate
      proxy = self.class.merged_configure_proxy

      if self.respond_to?(:owner) && self.owner
        owner_proxy = owner.class.merged_configure_proxy
        if proxy.configured_in_section
          owner_proxy = owner_proxy.sections[proxy.configured_in_section]
        end
        proxy.overwrite_defaults(owner_proxy) if owner_proxy
      end

      proxy
    end

    def configured_section_create(name, conf = nil)
      conf ||= Fluent::Config::Element.new(name.to_s, '', {}, [])
      root_proxy = configure_proxy_generate
      proxy = if name.nil? # root
                root_proxy
              else
                root_proxy.sections[name]
              end
      # take care to raise Fluent::ConfigError if conf mismatched to proxy
      Fluent::Config::SectionGenerator.generate(proxy, conf, nil, nil)
    end

    def configure(conf)
      @config = conf

      logger = if self.respond_to?(:log)
                 self.log
               elsif self.respond_to?(:owner) && self.owner.respond_to?(:log)
                 self.owner.log
               elsif defined?($log)
                 $log
               else
                 nil
               end
      proxy = configure_proxy_generate
      conf.corresponding_proxies << proxy

      # In the nested section, can't get plugin class through proxies so get plugin class here
      plugin_class = Fluent::Plugin.lookup_type_from_class(proxy.name.to_s)
      root = Fluent::Config::SectionGenerator.generate(proxy, conf, logger, plugin_class)
      @config_root_section = root

      root.instance_eval{ @params.keys }.each do |param_name|
        next if param_name.to_s.start_with?('@')
        varname = "@#{param_name}".to_sym
        instance_variable_set(varname, root[param_name])
      end

      self
    end

    def config
      @masked_config ||= @config.to_masked_element
    end

    CONFIG_TYPE_REGISTRY = Registry.new(:config_type, 'fluent/plugin/type_')

    def self.register_type(type, callable = nil, &block)
      callable ||= block
      CONFIG_TYPE_REGISTRY.register(type, callable)
    end

    def self.lookup_type(type)
      CONFIG_TYPE_REGISTRY.lookup(type)
    end

    {
      string: Config::STRING_TYPE,
      enum: Config::ENUM_TYPE,
      integer: Config::INTEGER_TYPE,
      float: Config::FLOAT_TYPE,
      size: Config::SIZE_TYPE,
      bool: Config::BOOL_TYPE,
      time: Config::TIME_TYPE,
      hash: Config::HASH_TYPE,
      array: Config::ARRAY_TYPE,
      regexp: Config::REGEXP_TYPE,
    }.each do |name, type|
      register_type(name, type)
    end

    module ClassMethods
      def configure_proxy_map
        map = {}
        self.define_singleton_method(:configure_proxy_map){ map }
        map
      end

      def configure_proxy(mod_name)
        map = configure_proxy_map
        unless map[mod_name]
          type_lookup = ->(type) { Fluent::Configurable.lookup_type(type) }
          proxy = Fluent::Config::ConfigureProxy.new(mod_name, root: true, required: true, multi: false, type_lookup: type_lookup)
          map[mod_name] = proxy
        end
        map[mod_name]
      end

      def configured_in(section_name)
        configure_proxy(self.name).configured_in(section_name)
      end

      def config_param(name, type = nil, **kwargs, &block)
        configure_proxy(self.name).config_param(name, type, **kwargs, &block)
        # reserved names '@foo' are invalid as attr_accessor name
        attr_accessor(name) unless kwargs[:skip_accessor] || Fluent::Config::Element::RESERVED_PARAMETERS.include?(name.to_s)
      end

      def config_set_default(name, defval)
        configure_proxy(self.name).config_set_default(name, defval)
      end

      def config_set_desc(name, desc)
        configure_proxy(self.name).config_set_desc(name, desc)
      end

      def config_section(name, **kwargs, &block)
        section_already_exists = !!merged_configure_proxy.sections[name]
        configure_proxy(self.name).config_section(name, **kwargs, &block)
        variable_name = configure_proxy(self.name).sections[name].variable_name
        if !section_already_exists && !self.respond_to?(variable_name)
          attr_accessor variable_name
        end
      end

      def desc(description)
        configure_proxy(self.name).desc(description)
      end

      def merged_configure_proxy
        configurables = ancestors.reverse.select{ |a| a.respond_to?(:configure_proxy) }

        # 'a.object_id.to_s' is to support anonymous class
        #   which created in tests to overwrite original behavior temporally
        #
        # p Module.new.name   #=> nil
        # p Class.new.name    #=> nil
        # p AnyGreatClass.dup.name #=> nil
        configurables.map{ |a| a.configure_proxy(a.name || a.object_id.to_s) }.reduce(:merge)
      end

      def dump_config_definition
        configure_proxy_map[self.to_s].dump_config_definition
      end
    end
  end
end
