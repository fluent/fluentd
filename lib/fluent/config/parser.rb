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

require 'uri'

require 'fluent/config/error'
require 'fluent/config/element'

module Fluent
  module Config
    class Parser
      def self.parse(io, fname, basepath = Dir.pwd)
        attrs, elems = Parser.new(basepath, io.each_line, fname).parse!(true)
        Element.new('ROOT', '', attrs, elems)
      end

      def initialize(basepath, iterator, fname, i = 0)
        @basepath = basepath
        @iterator = iterator
        @i = i
        @fname = fname
      end

      def parse!(allow_include, elem_name = nil, attrs = {}, elems = [])
        while line = @iterator.next
          line.force_encoding('UTF-8')
          @i += 1
          line.lstrip!
          line.gsub!(/\s*(?:\#.*)?$/,'')
          if line.empty?
            next
          elsif m = /^\<include\s*(.*)\s*\/\>$/.match(line)
            value = m[1].strip
            process_include(attrs, elems, value, allow_include)
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

      def process_include(attrs, elems, uri, allow_include = true)
        u = URI.parse(uri)
        if u.scheme == 'file' || u.path == uri  # file path
          path = u.path
          if path[0] != ?/
            pattern = File.expand_path("#{@basepath}/#{path}")
          else
            pattern = path
          end

          Dir.glob(pattern).sort.each { |entry|
            basepath = File.dirname(entry)
            fname = File.basename(entry)
            File.open(entry) { |f|
              Parser.new(basepath, f.each_line, fname).parse!(allow_include, nil, attrs, elems)
            }
          }

        else
          basepath = '/'
          fname = path
          require 'open-uri'
          open(uri) {|f|
            Parser.new(basepath, f.each_line, fname).parse!(allow_include, nil, attrs, elems)
          }
        end

      rescue SystemCallError => e
        raise ConfigParseError, "include error at #{@fname} line #{@i}: #{e.to_s}"
      end
    end
  end
end

