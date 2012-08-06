#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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
module Fluentd


  module Config
    class Element < Hash
      def initialize(name, arg, attrs, elements, used=[])
        @name = name
        @arg = arg
        @elements = elements
        super()
        attrs.each {|k,v|
          self[k] = v
        }
        @used = used
      end

      attr_accessor :name, :arg, :elements, :used

      def add_element(name, arg='')
        e = Element.new(name, arg, {}, [])
        @elements << e
        e
      end

      def +(o)
        Element.new(@name.dup, @arg.dup, o.merge(self), @elements+o.elements, @used+o.used)
      end

      def has_key?(key)
        @used << key
        super
      end

      def [](key)
        @used << key
        super
      end

      def check_not_fetched(&block)
        each_key {|key|
          unless @used.include?(key)
            block.call(key, self)
          end
        }
        @elements.each {|e|
          e.check_not_fetched(&block)
        }
      end

      def to_s(nest = 0)
        indent = "  "*nest
        nindent = "  "*(nest+1)
        out = ""
        if @arg.empty?
          out << "#{indent}<#{@name}>\n"
        else
          out << "#{indent}<#{@name} #{@arg}>\n"
        end
        each_pair {|k,v|
          out << "#{nindent}#{k} #{v}\n"
        }
        @elements.each {|e|
          out << e.to_s(nest+1)
        }
        out << "#{indent}</#{@name}>\n"
        out
      end
    end

    def self.evaluate(data)
      parse(data)
    end

    def self.read(path)
      Parser.read(path)
    end

    def self.parse(str, fname, basepath=Dir.pwd)
      Parser.parse(str, fname, basepath)
    end

    def self.new(name='')
      Element.new('', '', {}, [])
    end

    private
    class Parser
      def self.read(path)
        path = File.expand_path(path)
        File.open(path) {|io|
          parse(io, File.basename(path), File.dirname(path))
        }
      end

      def self.parse(io, fname, basepath=Dir.pwd)
        attrs, elems = Parser.new(basepath, io.each_line, fname).parse!(true)
        Element.new('ROOT', '', attrs, elems)
      end

      def initialize(basepath, iterator, fname, i=0)
        @basepath = basepath
        @iterator = iterator
        @i = i
        @fname = fname
      end

      def parse!(allow_include, elem_name=nil, attrs={}, elems=[])
        while line = @iterator.next
          @i += 1
          line.lstrip!
          line.gsub!(/\s*(?:\#.*)?$/,'')
            if line.empty?
              next
            elsif m = /^\<([a-zA-Z0-9_]+)\s*(.+?)?\>$/.match(line)
              e_name = m[1]
              e_arg = m[2] || ""
              e_attrs, e_elems = parse!(false, e_name)
              elems << Element.new(e_name, e_arg, e_attrs, e_elems)
            elsif line == "</#{elem_name}>"
              break
            elsif m = /^([a-zA-Z0-9_]+)\s*(.*)$/.match(line)
              key = m[1]
              value = m[2]
              if allow_include && key == 'include'
                process_include(attrs, elems, value)
              else
                attrs[key] = value
              end
              next
            else
              raise ConfigParseError, "parse error at #{@fname} line #{@i}"
            end
        end

        return attrs, elems
      rescue StopIteration
        return attrs, elems
      end

      def process_include(attrs, elems, uri)
        u = URI.parse(uri)
        if u.scheme == 'file' || u.path == uri  # file path
          path = u.path
          if path[0] != ?/
            pattern = File.expand_path("#{@basepath}/#{path}")
          else
            pattern = path
          end

          Dir.glob(pattern).each {|path|
            basepath = File.dirname(path)
            fname = File.basename(path)
            File.open(path) {|f|
              Parser.new(basepath, f.each_line, fname).parse!(true, nil, attrs, elems)
            }
          }

        else
          basepath = '/'
          fname = path
          require 'open-uri'
          open(uri) {|f|
            Parser.new(basepath, f.each_line, fname).parse!(true, nil, attrs, elems)
          }
        end

      rescue SystemCallError
        raise ConfigParseError, "include error at #{@fname} line #{@i}: #{$!.to_s}"
      end
    end
  end

end

