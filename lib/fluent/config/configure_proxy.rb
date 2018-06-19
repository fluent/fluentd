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
      attr_accessor :name, :final, :param_name, :init, :required, :multi, :alias, :configured_in_section
      attr_accessor :argument, :params, :defaults, :descriptions, :sections
      # config_param :desc, :string, default: '....'
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

      def initialize(name, root: false, param_name: nil, final: nil, init: nil, required: nil, multi: nil, alias: nil, type_lookup:)
        @name = name.to_sym
        @final = final

        # For ConfigureProxy of root section, "@name" should be a class name of plugins.
        # Otherwise (like subsections), "@name" should be a name of section, like "buffer", "store".
        # For subsections, name will be used as parameter names (unless param_name exists), so overriding proxy's name
        #   should override "@name".
        @root_section = root

        @param_name = param_name && param_name.to_sym
        @init = init
        @required = required
        @multi = multi
        @alias = binding.local_variable_get(:alias)
        @type_lookup = type_lookup

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

      def variable_name
        @param_name || @name
      end

      def root?
        @root_section
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

        [:param_name, :required, :multi, :alias, :configured_in_section].each do |prohibited_name|
          if overwrite?(other, prohibited_name)
            raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: #{prohibited_name}"
          end
        end

        options = {}
        # param_name affects instance variable name, which is just "internal" of each plugins.
        # so it must not be changed. base class's name (or param_name) is always used.
        options[:param_name] = @param_name

        # subclass cannot overwrite base class's definition
        options[:init] = @init.nil? ? other.init : self.init
        options[:required] = @required.nil? ? other.required : self.required
        options[:multi] = @multi.nil? ? other.multi : self.multi
        options[:alias] = @alias.nil? ? other.alias : self.alias
        options[:final] = @final || other.final
        options[:type_lookup] = @type_lookup

        merged = if self.root?
                   options[:root] = true
                   self.class.new(other.name, options)
                 else
                   self.class.new(@name, options)
                 end

        # configured_in MUST be kept
        merged.configured_in_section = self.configured_in_section || other.configured_in_section

        merged.argument = other.argument || self.argument
        merged.params = self.params.merge(other.params)
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
        #  * append params/defaults/sections which are missing in superclass
        #  * change default values of superclass
        #  * overwrite init to make it enable to instantiate section objects with added default values

        if other.final == false && overwrite?(other, :final)
          raise ConfigError, "BUG: subclass cannot overwrite finalized base class's config_section"
        end

        [:param_name, :required, :multi, :alias, :configured_in_section].each do |prohibited_name|
          if overwrite?(other, prohibited_name)
            raise ConfigError, "BUG: subclass cannot overwrite base class's config_section: #{prohibited_name}"
          end
        end

        options = {}
        options[:param_name] = @param_name
        options[:init] = @init || other.init
        options[:required] = @required.nil? ? other.required : self.required
        options[:multi] = @multi.nil? ? other.multi : self.multi
        options[:alias] = @alias.nil? ? other.alias : self.alias
        options[:final]  = true
        options[:type_lookup] = @type_lookup

        merged = if self.root?
                   options[:root] = true
                   self.class.new(other.name, options)
                 else
                   self.class.new(@name, options)
                 end

        merged.configured_in_section = self.configured_in_section || other.configured_in_section

        merged.argument = self.argument || other.argument
        merged.params = other.params.merge(self.params)
        merged.defaults = self.defaults.merge(other.defaults)
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

      def option_value_type!(name, opts, key, klass=nil, type: nil)
        if opts.has_key?(key)
          if klass && !opts[key].is_a?(klass)
            raise ArgumentError, "#{name}: #{key} must be a #{klass}, but #{opts[key].class}"
          end
          case type
          when :boolean
            unless opts[key].is_a?(TrueClass) || opts[key].is_a?(FalseClass)
              raise ArgumentError, "#{name}: #{key} must be true or false, but #{opts[key].class}"
            end
          when nil
            # ignore
          else
            raise "unknown type: #{type} for option #{key}"
          end
        end
      end

      def config_parameter_option_validate!(name, type, **kwargs, &block)
        if type.nil? && !block
          type = :string
        end
        kwargs.each_key do |key|
          case key
          when :default, :alias, :secret, :skip_accessor, :deprecated, :obsoleted, :desc
            # valid for all types
          when :list
            raise ArgumentError, ":list is valid only for :enum type, but #{type}: #{name}" if type != :enum
          when :value_type
            raise ArgumentError, ":value_type is valid only for :hash and :array, but #{type}: #{name}" if type != :hash && type != :array
          when :symbolize_keys
            raise ArgumentError, ":symbolize_keys is valid only for :hash, but #{type}: #{name}" if type != :hash
          else
            raise ArgumentError, "unknown option '#{key}' for configuration parameter: #{name}"
          end
        end
      end

      def parameter_configuration(name, type = nil, **kwargs, &block)
        config_parameter_option_validate!(name, type, **kwargs, &block)

        name = name.to_sym

        if block && type
          raise ArgumentError, "#{name}: both of block and type cannot be specified"
        elsif !block && !type
          type = :string
        end
        opts = {}
        opts[:type] = type
        opts.merge!(kwargs)

        begin
          block ||= @type_lookup.call(type)
        rescue ConfigError
          # override error message
          raise ArgumentError, "#{name}: unknown config_argument type `#{type}'"
        end

        # options for config_param
        option_value_type!(name, opts, :desc, String)
        option_value_type!(name, opts, :alias, Symbol)
        option_value_type!(name, opts, :secret, type: :boolean)
        option_value_type!(name, opts, :deprecated, String)
        option_value_type!(name, opts, :obsoleted, String)
        if type == :enum
          if !opts.has_key?(:list) || !opts[:list].is_a?(Array) || opts[:list].empty? || !opts[:list].all?{|v| v.is_a?(Symbol) }
            raise ArgumentError, "#{name}: enum parameter requires :list of Symbols"
          end
        end
        option_value_type!(name, opts, :symbolize_keys, type: :boolean)
        option_value_type!(name, opts, :value_type, Symbol) # hash, array
        option_value_type!(name, opts, :skip_accessor, type: :boolean)

        if opts.has_key?(:default)
          config_set_default(name, opts[:default])
        end

        if opts.has_key?(:desc)
          config_set_desc(name, opts[:desc])
        end

        if opts[:deprecated] && opts[:obsoleted]
          raise ArgumentError, "#{name}: both of deprecated and obsoleted cannot be specified at once"
        end

        [name, block, opts]
      end

      def configured_in(section_name)
        if @configured_in_section
          raise ArgumentError, "#{self.name}: configured_in called twice"
        end
        @configured_in_section = section_name.to_sym
      end

      def config_argument(name, type = nil, **kwargs, &block)
        if @argument
          raise ArgumentError, "#{self.name}: config_argument called twice"
        end
        name, block, opts = parameter_configuration(name, type, **kwargs, &block)

        @argument = [name, block, opts]
        name
      end

      def config_param(name, type = nil, **kwargs, &block)
        name, block, opts = parameter_configuration(name, type, **kwargs, &block)

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

      def config_section(name, **kwargs, &block)
        unless block_given?
          raise ArgumentError, "#{name}: config_section requires block parameter"
        end
        name = name.to_sym

        sub_proxy = ConfigureProxy.new(name, type_lookup: @type_lookup, **kwargs)
        sub_proxy.instance_exec(&block)

        @params.delete(name)
        @sections[name] = sub_proxy

        name
      end

      def dump_config_definition
        dumped_config = {}
        if @argument
          argument_name, _block, options = @argument
          options[:required] = !@defaults.key?(argument_name)
          options[:argument] = true
          dumped_config[argument_name] = options
        end
        @params.each do |name, config|
          dumped_config[name] = config[1]
          dumped_config[name][:required] = !@defaults.key?(name)
          dumped_config[name][:default] = @defaults[name] if @defaults.key?(name)
          dumped_config[name][:desc] = @descriptions[name] if @descriptions.key?(name)
        end
        # Overwrite by config_set_default
        @defaults.each do |name, value|
          if @params.key?(name) || (@argument && @argument.first == name)
            dumped_config[name][:default] = value
          else
            dumped_config[name] = { default: value }
          end
        end
        # Overwrite by config_set_desc
        @descriptions.each do |name, value|
          if @params.key?(name)
            dumped_config[name][:desc] = value
          else
            dumped_config[name] = { desc: value }
          end
        end
        @sections.each do |section_name, sub_proxy|
          if dumped_config.key?(section_name)
            dumped_config[section_name].update(sub_proxy.dump_config_definition)
          else
            dumped_config[section_name] = sub_proxy.dump_config_definition
            dumped_config[section_name][:required] = sub_proxy.required?
            dumped_config[section_name][:multi] = sub_proxy.multi?
            dumped_config[section_name][:alias] = sub_proxy.alias
            dumped_config[section_name][:section] = true
          end
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
