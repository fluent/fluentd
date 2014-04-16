module Fluent
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
        if root[param_name] || instance_variable_get(varname).nil?
          instance_variable_set(varname, root[param_name])
        end
      end
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
      end

      def configure_proxy(mod_name)
        unless configure_proxy_map[mod_name]
          proxy = ConfigureProxy.new(mod_name, required: true, multi: false)
          configure_proxy_map[mod_name] = proxy
        end
        configure_proxy_map[mod_name]
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
        configurables = ancestors.reverse.select{|a| a.respond_to?(:configure_proxy) }

        # 'a.object_id.to_s' is to support anonymous class
        #   which created in tests to overwrite original behavior temporally
        #
        # p Module.new.name   #=> nil
        # p Class.new.name    #=> nil
        # p AnyGreatClass.dup.name #=> nil
        configurables.map{|a| a.configure_proxy(a.name || a.object_id.to_s) }.reduce(:merge)
      end
    end

    class ConfigureProxy
      attr_accessor :name, :param_name, :required, :multi, :params, :defaults, :sections
      # config_param :desc, :string, :default => '....'
      # config_set_default :buffer_type, :memory
      #
      # config_section :default, required: true, multi: false do
      #   config_param :required, :bool, default: false
      #   config_param :name, :string
      #   config_param :power, :integer
      # end
      #
      # config_section :child, param_name: 'children', required: false, multi: true do
      #   config_param :name, :string
      #   config_param :power, :integer, default: nil
      #   config_section :item do
      #     config_param :name
      #   end
      # end

      def initialize(name, opts={})
        @name = name.to_sym

        @param_name = (opts[:param_name] || @name).to_sym
        @required = opts[:required]
        @multi = opts[:multi]

        @params = {}
        @defaults = {}
        @sections = {}
      end

      def required? ; @required.nil? ? false : @required ; end

      def multi? ; @multi.nil? ? true : @multi ; end

      def merge(other) # self is base class, other is subclass
        options = {
          param_name: other.param_name,
          required: ( other.required.nil? ? self.required : other.required ),
          multi: ( other.multi.nil? ? self.multi : other.multi)
        }
        merged = self.class.new(other.name, options)

        merged.params = self.params.merge(other.params)
        merged.defaults = self.defaults.merge(other.defaults)
        merged.sections = self.sections.merge(other.sections)

        merged
      end

      def config_param(name, *args, &block)
        name = name.to_sym

        opts = {}
        args.each { |a|
          if a.is_a?(Symbol)
            opts[:type] = a
          elsif a.is_a?(Hash)
            opts.merge!(a)
          else
            raise ArgumentError, "wrong number of arguments (#{1 + args.length} for #{block ? 2 : 3})"
          end
        }

        type = opts[:type]
        if block && type
          raise ArgumentError, "wrong number of arguments (#{1 + args.length} for 2)" # block exists
        end

        begin
          type = :string if type.nil?
          block ||= Configurable.lookup_type(type)
        rescue ConfigError
          # override error message
          raise ArgumentError, "unknown config_param type `#{type}'"
        end

        @params.delete(name)
        @sections.delete(name)
        @params[name] = [block, opts]

        if opts.has_key?(:default)
          config_set_default(name, opts[:default])
        end

        name
      end

      def config_set_default(name, defval)
        name = name.to_sym

        @defaults.delete(name)
        @defaults[name] = defval

        nil
      end

      def config_section(name, *args, &block)
        unless block_given?
          raise ArgumentError, "config_section requires block parameter"
        end
        name = name.to_sym

        opts = {}
        unless args.empty? || args.size == 1 && args.first.is_a?(Hash)
          raise ArgumentError, "unknown config_section arguments: #{args.inspect}"
        end

        sub_proxy = ConfigureProxy.new(name, *args)
        sub_proxy.instance_exec(&block)

        @params.delete(name)
        @sections.delete(name)
        @sections[name] = sub_proxy

        name
      end
    end
  end

  # load default types
  require 'fluent/config/types'
end
