module Fluent
  require 'fluent/config/configure_proxy'
  require 'fluent/config/section'
  require 'fluent/config/error'
  require 'fluent/registry'

  module Configurable
    attr_reader :config

    def self.included(mod)
      mod.extend(ClassMethods)
    end

    def initialize
      # to simulate implicit 'attr_accessor' by config_param / config_section and its value by config_set_default
      proxy = self.class.merged_configure_proxy
      proxy.params.keys.each do |name|
        if proxy.defaults.has_key?(name)
          instance_variable_set("@#{name}".to_sym, proxy.defaults[name])
        end
      end
      proxy.sections.keys.each do |name|
        subproxy = proxy.sections[name]
        if subproxy.multi?
          instance_variable_set("@#{subproxy.param_name}".to_sym, [])
        else
          instance_variable_set("@#{subproxy.param_name}".to_sym, nil)
        end
      end
    end

    def configure(conf)
      @config = conf

      logger = self.respond_to?(:log) ? log : $log
      proxy = self.class.merged_configure_proxy

      root = Fluent::Config::SectionGenerator.generate(proxy, conf, logger)
      @config_root_section = root

      root.instance_eval{ @params.keys }.each do |param_name|
        varname = "@#{param_name}".to_sym
        if (! root[param_name].nil?) || instance_variable_get(varname).nil?
          instance_variable_set(varname, root[param_name])
        end
      end

      self
    end

    CONFIG_TYPE_REGISTRY = Registry.new(:config_type, 'fluent/plugin/type_')

    def self.register_type(type, callable = nil, &block)
      callable ||= block
      CONFIG_TYPE_REGISTRY.register(type, callable)
    end

    def self.lookup_type(type)
      CONFIG_TYPE_REGISTRY.lookup(type)
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
          proxy = Fluent::Config::ConfigureProxy.new(mod_name, required: true, multi: false)
          map[mod_name] = proxy
        end
        map[mod_name]
      end

      def config_param(name, *args, &block)
        configure_proxy(self.name).config_param(name, *args, &block)
        attr_accessor name
      end

      def config_set_default(name, defval)
        configure_proxy(self.name).config_set_default(name, defval)
      end

      def config_section(name, *args, &block)
        configure_proxy(self.name).config_section(name, *args, &block)
        attr_accessor configure_proxy(self.name).sections[name].param_name
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
    end
  end

  # load default types
  require 'fluent/config/types'
end
