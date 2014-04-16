module Fluent
  module Config
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
end

