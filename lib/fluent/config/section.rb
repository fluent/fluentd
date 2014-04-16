module Fluent
  require 'fluent/config/error'

  module Config
    module SectionGenerator
      def self.generate(proxy, conf, logger, stack=[])
        return nil if conf.nil?

        section_stack = ""
        unless stack.empty?
          section_stack = ", in section " + stack.join(" > ")
        end

        section_params = {}

        proxy.defaults.each_pair do |name, defval|
          varname = name.to_sym
          section_params[varname] = defval
        end

        proxy.params.each_pair do |name, defval|
          varname = name.to_sym
          block,opts = defval
          if val = conf[name.to_s]
            section_params[varname] = self.instance_exec(val, opts, name, &block)
          end
          unless section_params.has_key?(varname)
            logger.error "config error in:\n#{conf}"
            raise ConfigError, "'#{name}' parameter is required" + section_stack
          end
        end

        proxy.sections.each do |name, subproxy|
          varname = subproxy.param_name.to_sym
          elements = (conf.respond_to?(:elements) ? conf.elements : []).select{|e| e.name == subproxy.name.to_s }

          if subproxy.required? && elements.size < 1
            logger.error "config error in:\n#{conf}"
            raise ConfigError, "'<#{subproxy.name}>' sections are required" + section_stack
          end
          if subproxy.multi?
            section_params[varname] = elements.map{ |e| generate(proxy.h, e, logger, stack + [subproxy.name]) }
          else
            if elements.size > 1
              logger.error "config error in:\n#{conf}"
              raise ConfigError, "'<#{subproxy.name}>' section cannot be written twice or more" + section_stack
            end
            section_params[varname] = generate(proxy, elements.first, logger, stack + [subproxy.name])
          end
        end

        Section.new(section_params)
      end
    end

    class Section < BasicObject
      # to get parameter list, see instance methods
      def initialize(params)
        @params = params
      end

      def [](key)
        @params[key.to_sym]
      end

      def respond_to_missing?(symbol, include_private)
        @params.has_key?(symbol)
      end

      def method_missing(name, *args)
        if @params.has_key?(name)
          @params[name]
        else
          super
        end
      end
    end
  end
end
