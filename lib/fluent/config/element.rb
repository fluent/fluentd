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
  require 'fluent/config/error'

  module Config
    class Element < Hash
      def initialize(name, arg, attrs, elements, system_attrs, unused = nil)
        @name = name
        @arg = arg
        @elements = elements
        @system = system_attrs
        super()
        attrs.each {|k, v|
          self[k] = v
        }
        @unused = unused || attrs.keys
        @v1_config = false
      end

      attr_accessor :name, :arg, :elements, :system, :unused, :v1_config

      def add_element(name, arg='')
        e = Element.new(name, arg, {}, [])
        e.v1_config = @v1_config
        @elements << e
        e
      end

      def inspect
        attrs = super
        "name:#{@name}, arg:#{@arg}, " + attrs + ", " + @elements.inspect + ", system:#{@system}"
      end

      def ==(o)
        self.name == o.name && self.arg == o.arg &&
          self.keys.size == o.keys.size &&
          self.keys.reduce(true){|r, k| r && self[k] == o[k] } &&
          self.elements.size == o.elements.size &&
          [self.elements, o.elements].transpose.reduce(true){|r, e| r && e[0] == e[1] } &&
          self.system.keys.size == o.system.keys.size &&
          self.system.keys.reduce(true){|r, k| r && self.system[k] == o.system[k] }
      end

      def +(o)
        e = Element.new(@name.dup, @arg.dup, o.merge(self), @elements + o.elements, o.system.merge(@system), (@unused + o.unused).uniq)
        e.v1_config = @v1_config
        e
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
        @system.each_pair { |k, v|
          out << "#{nindent}#{k} #{v}\n"
        }
        each_pair { |k, v|
          out << "#{nindent}#{k} #{v}\n"
        }
        @elements.each { |e|
          out << e.to_s(nest + 1)
        }
        out << "#{indent}</#{@name}>\n"
        out
      end

      def self.unescape_parameter(v)
        result = ''
        v.each_char { |c| result << LiteralParser.unescape_char(c) }
        result
      end
    end
  end
end
