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

require 'json'

require 'fluent/config/error'
require 'fluent/config/v1_parser'

module Fluent
  module Config
    class Section < BasicObject
      def self.name
        'Fluent::Config::Section'
      end

      def initialize(params = {}, config_element = nil)
        @klass = 'Fluent::Config::Section'
        @params = params
        @corresponding_config_element = config_element
      end

      alias :object_id :__id__

      def corresponding_config_element
        @corresponding_config_element
      end

      def class
        Section
      end

      def to_s
        inspect
      end

      def inspect
        "<Fluent::Config::Section #{@params.to_json}>"
      end

      def nil?
        false
      end

      def to_h
        @params
      end

      def +(other)
        Section.new(self.to_h.merge(other.to_h))
      end

      def instance_of?(mod)
        @klass == mod.name
      end

      def kind_of?(mod)
        @klass == mod.name || BasicObject == mod
      end
      alias is_a? kind_of?

      def [](key)
        @params[key.to_sym]
      end

      def []=(key, value)
        @params[key.to_sym] = value
      end

      def respond_to?(symbol, include_all=false)
        case symbol
        when :inspect, :nil?, :to_h, :+, :instance_of?, :kind_of?, :[], :respond_to?, :respond_to_missing?
          true
        when :!, :!= , :==, :equal?, :instance_eval, :instance_exec
          true
        when :method_missing, :singleton_method_added, :singleton_method_removed, :singleton_method_undefined
          include_all
        else
          false
        end
      end

      def respond_to_missing?(symbol, include_private)
        @params.has_key?(symbol)
      end

      def method_missing(name, *args)
        if @params.has_key?(name)
          @params[name]
        else
          ::Kernel.raise ::NoMethodError, "undefined method `#{name}' for #{self.inspect}"
        end
      end
    end

    module SectionGenerator
      def self.generate(proxy, conf, logger, plugin_class, stack = [])
        return nil if conf.nil?

        section_stack = ""
        unless stack.empty?
          section_stack = ", in section " + stack.join(" > ")
        end

        section_params = {}

        proxy.defaults.each_pair do |name, defval|
          varname = name.to_sym
          section_params[varname] = (defval.dup rescue defval)
        end

        if proxy.argument
          unless conf.arg.empty?
            key, block, opts = proxy.argument
            section_params[key] = self.instance_exec(conf.arg, opts, name, &block)
          end
          unless section_params.has_key?(proxy.argument.first)
            logger.error "config error in:\n#{conf}" if logger # logger should exist, but somethimes it's nil (e.g, in tests)
            raise ConfigError, "'<#{proxy.name} ARG>' section requires argument" + section_stack
          end
          # argument should NOT be deprecated... (argument always has a value: '')
        end

        proxy.params.each_pair do |name, defval|
          varname = name.to_sym
          block, opts = defval
          if conf.has_key?(name.to_s) || opts[:alias] && conf.has_key?(opts[:alias].to_s)
            val = if conf.has_key?(name.to_s)
                    conf[name.to_s]
                  else
                    conf[opts[:alias].to_s]
                  end
            section_params[varname] = self.instance_exec(val, opts, name, &block)

            # Source of definitions of deprecated/obsoleted:
            # https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Deprecated_and_obsolete_features
            #
            # Deprecated: These deprecated features can still be used, but should be used with caution
            #             because they are expected to be removed entirely sometime in the future.
            # Obsoleted: These obsolete features have been entirely removed from JavaScript and can no longer be used.
            if opts[:deprecated]
              logger.warn "'#{name}' parameter is deprecated: #{opts[:deprecated]}" if logger
            end
            if opts[:obsoleted]
              logger.error "config error in:\n#{conf}" if logger
              raise ObsoletedParameterError, "'#{name}' parameter is already removed: #{opts[:obsoleted]}" + section_stack
            end
          end
          unless section_params.has_key?(varname)
            logger.error "config error in:\n#{conf}" if logger
            raise ConfigError, "'#{name}' parameter is required" + section_stack
          end
        end

        check_unused_section(proxy, conf, plugin_class)

        proxy.sections.each do |name, subproxy|
          varname = subproxy.variable_name
          elements = (conf.respond_to?(:elements) ? conf.elements : []).select{ |e| e.name == subproxy.name.to_s || e.name == subproxy.alias.to_s }
          if elements.empty? && subproxy.init?
            if subproxy.argument && !subproxy.defaults.has_key?(subproxy.argument.first)
              raise ArgumentError, "#{name}: init is specified, but default value of argument is missing"
            end
            missing_keys = subproxy.params.keys.select{|param_name| !subproxy.defaults.has_key?(param_name)}
            if !missing_keys.empty?
              raise ArgumentError, "#{name}: init is specified, but there're parameters without default values:#{missing_keys.join(',')}"
            end
            elements << Fluent::Config::Element.new(subproxy.name.to_s, '', {}, [])
          end

          # set subproxy for secret option
          elements.each { |element|
            element.corresponding_proxies << subproxy
          }

          if subproxy.required? && elements.size < 1
            logger.error "config error in:\n#{conf}" if logger
            raise ConfigError, "'<#{subproxy.name}>' sections are required" + section_stack
          end
          if subproxy.multi?
            section_params[varname] = elements.map{ |e| generate(subproxy, e, logger, plugin_class, stack + [subproxy.name]) }
          else
            if elements.size > 1
              logger.error "config error in:\n#{conf}" if logger
              raise ConfigError, "'<#{subproxy.name}>' section cannot be written twice or more" + section_stack
            end
            section_params[varname] = generate(subproxy, elements.first, logger, plugin_class, stack + [subproxy.name])
          end
        end

        Section.new(section_params, conf)
      end

      def self.check_unused_section(proxy, conf, plugin_class)
        elems = conf.respond_to?(:elements) ? conf.elements : []
        elems.each { |e|
          next if plugin_class.nil? && Fluent::Config::V1Parser::ELEM_SYMBOLS.include?(e.name) # skip pre-defined non-plugin elements because it doens't have proxy section

          unless proxy.sections.any? { |name, subproxy| e.name == subproxy.name.to_s || e.name == subproxy.alias.to_s }
            parent_name = if conf.arg.empty?
                            conf.name
                          else
                            "#{conf.name} #{conf.arg}"
                          end
            e.unused_in = [parent_name, plugin_class]
          end
        }
      end
    end
  end
end
