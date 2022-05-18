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
    module YamlParser
      SectionBuilder = Struct.new(:name, :body, :indent_size, :arg) do
        def to_s
          indent = ' ' * indent_size

          if arg && !arg.to_s.empty?
            "#{indent}<#{name} #{arg}>\n#{body}\n#{indent}</#{name}>"
          else
            "#{indent}<#{name}>\n#{body}\n#{indent}</#{name}>"
          end
        end

        def to_element
          elem = body.to_element
          elem.name = name
          elem.arg = arg.to_s if arg
          elem.v1_config = true
          elem
        end
      end

      class RootBuilder
        def initialize(system, conf)
          @system = system
          @conf = conf
        end

        attr_reader :system, :conf

        def to_element
          Fluent::Config::Element.new('ROOT', '', {}, [@system, @conf].compact.map(&:to_element).flatten)
        end

        def to_s
          s = StringIO.new(+'')
          s.puts(@system.to_s) if @system
          s.puts(@conf.to_s) if @conf

          s.string
        end
      end

      class SectionBodyBuilder
        Row = Struct.new(:key, :value, :indent) do
          def to_s
            "#{indent}#{key} #{value}"
          end
        end

        def initialize(indent, root: false)
          @indent = ' ' * indent
          @bodies = []
          @root = root
        end

        def add_line(k, v)
          @bodies << Row.new(k, v, @indent)
        end

        def add_section(section)
          @bodies << section
        end

        def to_element
          if @root
            return @bodies.map(&:to_element)
          end

          not_section, section = @bodies.partition { |e| e.is_a?(Row) }
          r = {}
          not_section.each do |e|
            v = e.value
            r[e.key] = v.respond_to?(:to_element) ? v.to_element : v
          end

          if @root
            section.map(&:to_element)
          else
            Fluent::Config::Element.new('', '', r, section.map(&:to_element))
          end
        end

        def to_s
          @bodies.map(&:to_s).join("\n")
        end
      end
    end
  end
end
