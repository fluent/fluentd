require 'fluent/config'

module Fluent
  module Config
    module DSL
      module Parser
        def self.read(path)
          path = File.expand_path(path)
          data = File.read(path)
          parse(data, path)
        end

        def self.parse(source, source_path="config.rb")
          Proxy.new('ROOT', nil).eval(source, source_path).to_config_element
        end
      end

      class Proxy
        def initialize(name, arg)
          @element = Element.new(name, arg, self)
        end

        def element
          @element
        end

        def eval(source, source_path)
          @element.instance_eval(source, source_path)
          self
        end

        def to_config_element
          @element.instance_eval do
            Config::Element.new(@name, @arg, @attrs, @elements)
          end
        end

        def add_element(name, arg, block)
          ::Kernel.raise ::ArgumentError, "#{name} block must be specified" if block.nil?

          proxy = self.class.new(name.to_s, arg)
          proxy.element.instance_exec(&block)

          @element.instance_eval do
            @elements.push(proxy.to_config_element)
          end

          self
        end
      end

      class Element < BasicObject
        def initialize(name, arg, proxy)
          @name     = name
          @arg      = arg || ''
          @attrs    = {}
          @elements = []
          @proxy    = proxy
        end

        def method_missing(name, *args, &block)
          ::Kernel.raise ::ArgumentError, "Configuration DSL Syntax Error: only one argument allowed" if args.size > 1
          value = args.first

          if block
            proxy = Proxy.new(name.to_s, value)
            proxy.element.instance_exec(&block)
            @elements.push(proxy.to_config_element)
          else
            @attrs[name.to_s] = value.to_s
          end

          self
        end

        def source(&block)
          @proxy.add_element('source', nil, block)
        end

        def match(*args, &block)
          ::Kernel.raise ::ArgumentError, "#{name} block requires arguments for match pattern" if args.nil? || args.size != 1
          @proxy.add_element('match', args.first, block)
        end
      end
    end
  end
end
