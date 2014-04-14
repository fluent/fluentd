module Fluent
  require 'fluent/config/error'
  require 'fluent/registry'

  module Configurable
    attr_reader :config

    def self.included(mod)
      mod.extend(ClassMethods)
    end

    def initialize
      self.class.config_defaults.each_pair { |name, defval|
        varname = :"@#{name}"
        instance_variable_set(varname, defval)
      }
    end

    def configure(conf)
      @config = conf

      self.class.config_params.each_pair { |name, (block,opts)|
        varname = :"@#{name}"
        if val = conf[name.to_s]
          val = self.instance_exec(val, opts, name, &block)
          instance_variable_set(varname, val)
        end
        unless instance_variable_defined?(varname)
          $log.error "config error in:\n#{conf}"
          raise ConfigError, "'#{name}' parameter is required"
        end
      }
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
          raise ArgumentError, "wrong number of arguments (#{1 + args.length} for #{block ? 2 : 3})"
        end

        begin
          type = :string if type.nil?
          block ||= Configurable.lookup_type(type)
        rescue ConfigError
          # override error message
          raise ArgumentError, "unknown config_param type `#{type}'"
        end

        params = config_params_set
        params.delete(name)
        params[name] = [block, opts]

        if opts.has_key?(:default)
          config_set_default(name, opts[:default])
        end

        attr_accessor name
      end

      def config_set_default(name, defval)
        name = name.to_sym

        defaults = config_defaults_set
        defaults.delete(name)
        defaults[name] = defval

        nil
      end

      def config_params
        singleton_value(:_config_params)
      end

      def config_defaults
        singleton_value(:_config_defaults)
      end

      private
      def config_params_set
        singleton_value_set(:_config_params)
      end

      def config_defaults_set
        singleton_value_set(:_config_defaults)
      end

      def singleton_value_set(name)
        if methods(false).include?(name)
          __send__(name)
        else
          val = {}
          define_singleton_method(name) { val }
          val
        end
      end

      def singleton_value(name)
        val = {}
        ancestors.reverse_each { |c|
          if c.methods(false).include?(name)
            val.merge!(c.__send__(name))
          end
        }
        val
      end
    end
  end

  # load default types
  require 'fluent/config/types'
end
