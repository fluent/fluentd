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

module Fluent
  module Config
    class ConfigureProxy
      attr_accessor :name, :param_name, :required, :multi, :alias, :argument, :params, :defaults, :sections
      # config_param :desc, :string, :default => '....'
      # config_set_default :buffer_type, :memory
      #
      # config_section :default, required: true, multi: false do
      #   config_argument :arg, :string
      #   config_param :required, :bool, default: false
      #   config_param :name, :string
      #   config_param :power, :integer
      # end
      #
      # config_section :child, param_name: 'children', required: false, multi: true, alias: 'node' do
      #   config_param :name, :string
      #   config_param :power, :integer, default: nil
      #   config_section :item do
      #     config_param :name
      #   end
      # end

      def initialize(name, opts = {})
        @name = name.to_sym

        @param_name = (opts[:param_name] || @name).to_sym
        @required = opts[:required]
        @multi = opts[:multi]
        @alias = opts[:alias]

        @argument = nil # nil: ignore argument
        @params = {}
        @defaults = {}
        @sections = {}
      end

      def required?
        @required.nil? ? false : @required
      end

      def multi?
        @multi.nil? ? true : @multi
      end

      def merge(other) # self is base class, other is subclass
        options = {
          param_name: other.param_name, # param_name is used not to ovewrite plugin's instance varible, so this should be able to be overwritten
          required: (@required.nil? ? other.required : self.required), # subclass cannot overwrite base class's definition
          multi: (@multi.nil? ? other.multi : self.multi),
          alias: (@alias.nil? ? other.alias : self.alias),
        }
        merged = self.class.new(other.name, options)

        merged.argument = other.argument || self.argument
        merged.params = other.params.merge(self.params)
        merged.defaults = self.defaults.merge(other.defaults)
        merged.sections = {}
        (self.sections.keys + other.sections.keys).uniq.each do |section_key|
          self_section = self.sections[section_key]
          other_section = other.sections[section_key]
          merged_section = if self_section && other_section
                             self_section.merge(other_section)
                           elsif self_section || other_section
                             self_section || other_section
                           else
                             raise "BUG: both of self and other section are nil"
                           end
          merged.sections[section_key] = merged_section
        end

        merged
      end

      def parameter_configuration(name, *args, &block)
        name = name.to_sym

        opts = {}
        args.each { |a|
          if a.is_a?(Symbol)
            opts[:type] = a
          elsif a.is_a?(Hash)
            opts.merge!(a)
          else
            raise ArgumentError, "#{self.name}: wrong number of arguments (#{1 + args.length} for #{block ? 2 : 3})"
          end
        }

        type = opts[:type]
        if block && type
          raise ArgumentError, "#{self.name}: both of block and type cannot be specified"
        end

        begin
          type = :string if type.nil?
          block ||= Configurable.lookup_type(type)
        rescue ConfigError
          # override error message
          raise ArgumentError, "#{self.name}: unknown config_argument type `#{type}'"
        end

        if opts.has_key?(:default)
          config_set_default(name, opts[:default])
        end

        [name, block, opts]
      end

      def config_argument(name, *args, &block)
        if @argument
          raise ArgumentError, "#{self.name}: config_argument called twice"
        end
        name, block, opts = parameter_configuration(name, *args, &block)

        @argument = [name, block, opts]
        name
      end

      def config_param(name, *args, &block)
        name, block, opts = parameter_configuration(name, *args, &block)

        @sections.delete(name)
        @params[name] = [block, opts]
        name
      end

      def config_set_default(name, defval)
        name = name.to_sym

        if @defaults.has_key?(name)
          raise ArgumentError, "#{self.name}: default value specified twice for #{name}"
        end

        @defaults[name] = defval
        nil
      end

      def config_section(name, *args, &block)
        unless block_given?
          raise ArgumentError, "#{self.name}: config_section requires block parameter"
        end
        name = name.to_sym

        opts = {}
        unless args.empty? || args.size == 1 && args.first.is_a?(Hash)
          raise ArgumentError, "#{self.name}: unknown config_section arguments: #{args.inspect}"
        end

        sub_proxy = ConfigureProxy.new(name, *args)
        sub_proxy.instance_exec(&block)

        @params.delete(name)
        @sections[name] = sub_proxy

        name
      end
    end
  end
end

