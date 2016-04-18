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

require 'fluent/configurable'

module Fluent
  module Config
    class ConfigureProxy
      attr_accessor :name, :final, :param_name, :init, :required, :multi, :alias, :configured_in_section
      attr_accessor :argument, :params, :defaults, :descriptions, :sections
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
        @final = opts[:final]

        @param_name = (opts[:param_name] || @name).to_sym
        @init = opts[:init]
        @required = opts[:required]
        @multi = opts[:multi]
        @alias = opts[:alias]

        raise "init and required are exclusive" if @init && @required

        # specify section name for viewpoint of owner(parent) plugin
        # for buffer plugins: all params are in <buffer> section of owner
        # others: <storage>, <format> (formatter/parser), ...
        @configured_in_section = nil

        @argument = nil # nil: ignore argument
        @params = {}
        @defaults = {}
        @descriptions = {}
        @sections = {}
        @current_description = nil
      end

      def init?
        @init.nil? ? false : @init
      end

      def required?
        @required.nil? ? false : @required
      end

      def multi?
        @multi.nil? ? true : @multi
      end

      def final?
        !!@final
      end

      def merge(other) # self is base class, other is subclass
        return merge_for_finalized(other) if self.final?

        if overwrite?(other, :init)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: init"
        end
        if overwrite?(other, :required)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: required"
        end
        if overwrite?(other, :multi)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: multi"
        end
        if overwrite?(other, :alias)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: alias"
        end
        if overwrite?(other, :configured_in_section)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: configured_in"
        end

        options = {}
        # param_name is used not to ovewrite plugin's instance
        # varible, so this should be able to be overwritten
        options[:param_name] = other.param_name
        # subclass cannot overwrite base class's definition
        options[:init] = @init.nil? ? other.init : self.init
        options[:required] = @required.nil? ? other.required : self.required
        options[:multi] = @multi.nil? ? other.multi : self.multi
        options[:alias] = @alias.nil? ? other.alias : self.alias
        options[:final] = @final || other.final

        merged = self.class.new(other.name, options)

        # configured_in MUST be kept
        merged.configured_in_section = self.configured_in_section

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

      def merge_for_finalized(other)
        # list what subclass can do for finalized section
        #  * overwrite param_name to escape duplicated name of instance variable
        #  * append params/defaults/sections which are missing in superclass

        if other.final == false && overwrite?(other, :final)
          raise ConfigError, "BUG: subclass cannot overwrite finalized base class's config_section"
        end

        if overwrite?(other, :init)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: init"
        end
        if overwrite?(other, :required)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: required"
        end
        if overwrite?(other, :multi)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: multi"
        end
        if overwrite?(other, :alias)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: alias"
        end
        if overwrite?(other, :configured_in_section)
          raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: configured_in"
        end

        options = {}
        options[:param_name] = other.param_name
        options[:init] = @init.nil? ? other.init : self.init
        options[:required] = @required.nil? ? other.required : self.required
        options[:multi] = @multi.nil? ? other.multi : self.multi
        options[:alias] = @alias.nil? ? other.alias : self.alias
        options[:final]  = true

        merged = self.class.new(other.name, options)

        merged.configured_in_section = self.configured_in_section

        merged.argument = self.argument || other.argument
        merged.params = other.params.merge(self.params)
        merged.defaults = other.defaults.merge(self.defaults)
        merged.sections = {}
        (self.sections.keys + other.sections.keys).uniq.each do |section_key|
          self_section = self.sections[section_key]
          other_section = other.sections[section_key]
          merged_section = if self_section && other_section
                             other_section.merge(self_section)
                           elsif self_section || other_section
                             self_section || other_section
                           else
                             raise "BUG: both of self and other section are nil"
                           end
          merged.sections[section_key] = merged_section
        end

        merged
      end

      def overwrite_defaults(other) # other is owner plugin's corresponding proxy
        self.defaults = self.defaults.merge(other.defaults)
        self.sections.keys.each do |section_key|
          if other.sections.has_key?(section_key)
            self.sections[section_key].overwrite_defaults(other.sections[section_key])
          end
        end
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

        if opts.has_key?(:desc)
          config_set_desc(name, opts[:desc])
        end

        [name, block, opts]
      end

      def configured_in(section_name)
        if @configured_in_section
          raise ArgumentError, "#{self.name}: configured_in called twice"
        end
        @configured_in_section = section_name.to_sym
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

        if @current_description
          config_set_desc(name, @current_description)
          @current_description = nil
        end

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

      def config_set_desc(name, description)
        name = name.to_sym

        if @descriptions.has_key?(name)
          raise ArgumentError, "#{self.name}: description specified twice for #{name}"
        end

        @descriptions[name] = description
        nil
      end

      def desc(description)
        @current_description = description
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

        if sub_proxy.init?
          if sub_proxy.argument && !sub_proxy.defaults.has_key?(sub_proxy.argument.first)
            raise ArgumentError, "#{self.name}: init is specified, but default value of argument is missing"
          end
          if sub_proxy.params.keys.any?{|param_name| !sub_proxy.defaults.has_key?(param_name)}
            raise ArgumentError, "#{self.name}: init is specified, but there're parameters without default values"
          end
        end

        @params.delete(name)
        @sections[name] = sub_proxy

        name
      end

      def dump(level = 0)
        dumped_config = ""
        indent = " " * level
        @params.each do |name, config|
          dumped_config << "#{indent}#{name}: #{config[1][:type]}: <#{@defaults[name].inspect}>"
          dumped_config << " # #{@descriptions[name]}" if @descriptions[name]
          dumped_config << "\n"
        end
        @sections.each do |section_name, sub_proxy|
          dumped_config << "#{indent}#{section_name}\n#{sub_proxy.dump(level + 1)}"
        end
        dumped_config
      end

      private

      def overwrite?(other, attribute_name)
        value = instance_variable_get("@#{attribute_name}")
        other_value = other.__send__(attribute_name)
        !value.nil? && !other_value.nil? && value != other_value
      end
    end
  end
end

