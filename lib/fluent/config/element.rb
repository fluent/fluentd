module Fluent
  require 'fluent/config/error'

  module Config
    class Element < Hash
      def initialize(name, arg, attrs, elements, unused = nil)
        @name = name
        @arg = arg
        @elements = elements
        super()
        attrs.each { |k, v|
          self[k] = v
        }
        @unused = unused || attrs.keys
      end

      attr_accessor :name, :arg, :elements, :unused

      def add_element(name, arg='')
        e = Element.new(name, arg, {}, [])
        @elements << e
        e
      end

      def +(o)
        Element.new(@name.dup, @arg.dup, o.merge(self), @elements + o.elements, (@unused + o.unused).uniq)
      end

      def each_element(*names, &block)
        if names.empty?
          @elements.each(&block)
        else
          @elements.each { |e|
            if names.include?(e.name)
              block.yield(e)
            end
          }
        end
      end

      def has_key?(key)
        @unused.delete(key)
        super
      end

      def [](key)
        @unused.delete(key)
        super
      end

      def check_not_fetched(&block)
        each_key { |key|
          if @unused.include?(key)
            block.call(key, self)
          end
        }
        @elements.each { |e|
          e.check_not_fetched(&block)
        }
      end

      def to_s(nest = 0)
        indent = "  " * nest
        nindent = "  " * (nest + 1)
        out = ""
        if @arg.empty?
          out << "#{indent}<#{@name}>\n"
        else
          out << "#{indent}<#{@name} #{@arg}>\n"
        end
        each_pair { |k, v|
          out << "#{nindent}#{k} #{v}\n"
        }
        @elements.each { |e|
          out << e.to_s(nest + 1)
        }
        out << "#{indent}</#{@name}>\n"
        out
      end
    end
  end
end
