module Fluentd
  module Config
    module DSL
      module DSLParser
        def self.read(path)
          path = File.expand_path(path)
          data = File.read(path)
          parse(data, path)
        end

        def self.parse(source, source_path="config.rb")
          DSLElement.new('ROOT', nil).instance_eval(source, source_path).__to_config_element
        end
      end

      class DSLElement
        attr_accessor :name, :arg, :attrs, :elements

        def initialize(name, arg)
          @name = name
          @arg = arg
          @attrs = {}
          @elements = []
        end

        def __to_config_element
          Fluentd::Config::Element.new(@name, @arg, @attrs, @elements)
        end

        def method_missing(name, *args, &block)
          raise ArgumentError, "Configuration DSL Syntax Error: only one argument allowed" if args.size > 1
          value = args.first
          if block
            element = DSLElement.new(name.to_s, value)
            element.instance_exec(&block)
            @elements.push(element.__to_config_element)
          else
            @attrs[name.to_s] = value.is_a?(Symbol) ? value.to_s : value
          end
          self
        end

        def worker(&block); element('worker', nil, block); end
        def source(&block); element('source', nil, block); end
        def filter(*args, &block); need_arg('filter', args); element('filter', args.first, block); end
        def match(*args, &block);  need_arg('match',  args); element('match',  args.first, block); end
        def label(*args, &block);  need_arg('label',  args); element('label',  args.first, block); end

        private

        def element(name, arg, block)
          raise ArgumentError, "#{name} block must be specified" if block.nil?
          element = DSLElement.new(name.to_s, arg)
          element.instance_exec(&block)
          @elements.push(element.__to_config_element)
          self
        end

        def need_arg(name, args)
          raise ArgumentError, "#{name} block requires arguments for match pattern" if args.nil? || args.size != 1
          true
        end
      end
    end
  end
end
