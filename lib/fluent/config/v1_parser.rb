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

    require 'strscan'
    require 'fluent/config/error'
    require 'fluent/config/literal_parser'
    require 'fluent/config/element'

    class V1Parser < LiteralParser
      ELEMENT_NAME = /[a-zA-Z0-9_]+/

      def self.read(path, eval_context = nil)
        path = File.expand_path(path)
        data = File.read(path)
        parse(data, File.basename(path), File.dirname(path), eval_context)
      end

      def self.parse(data, fname, basepath = Dir.pwd, eval_context = nil)
        ss = StringScanner.new(data)
        ps = V1Parser.new(ss, basepath, fname, eval_context)
        ps.parse!
      end

      def initialize(strscan, include_basepath, fname, eval_context)
        super(strscan, eval_context)
        @include_basepath = include_basepath
        @fname = fname
      end

      def parse!
        attrs, elems = parse_element(true, nil)
        root = Element.new('ROOT', '', attrs, elems)

        spacing
        unless eof?
          parse_error! "expected EOF"
        end

        return root
      end

      def parse_element(root_element, elem_name, attrs = {}, elems = [])
        while true
          spacing
          if eof?
            if root_element
              break
            end
            parse_error! "expected end tag '</#{elem_name}>' but got end of file"
          end

          if skip(/\<\//)
            e_name = scan(ELEMENT_NAME)
            spacing
            unless skip(/\>/)
              parse_error! "expected character in tag name"
            end
            unless line_end
              parse_error! "expected end of line after end tag"
            end
            if e_name != elem_name
              parse_error! "unmatched end tag"
            end
            break

          elsif skip(/\</)
            e_name = scan(ELEMENT_NAME)
            spacing
            e_arg = scan_nonquoted_string(/(?:#{SPACING}|\>)/)
            spacing
            unless skip(/\>/)
              parse_error! "expected '>'"
            end
            unless line_end
              parse_error! "expected end of line after tag"
            end
            e_arg ||= ''
            # call parse_element recursively
            e_attrs, e_elems = parse_element(false, e_name)
            elems << Element.new(e_name, e_arg, e_attrs, e_elems)

          elsif root_element && skip(/\@include#{SPACING}/)
            uri = scan_string(LINE_END)
            eval_include(attrs, elems, uri)
            line_end

          else
            k = scan_string(SPACING)
            spacing
            v = parse_literal
            unless line_end
              parse_error! "expected end of line"
            end
            attrs[k] = v
          end
        end

        return attrs, elems
      end

      def eval_include(attrs, elems, uri)
        u = URI.parse(uri)
        if u.scheme == 'file' || u.path == uri  # file path
          path = u.path
          if path[0] != ?/
            pattern = File.expand_path("#{@include_basepath}/#{path}")
          else
            pattern = path
          end

          Dir.glob(pattern).each { |path|
            basepath = File.dirname(path)
            fname = File.basename(path)
            data = File.read(path)
            ss = StringScanner.new(data)
            V1Parser.new(ss, basepath, fname, @eval_context).parse(true, nil, attrs, elems)
          }

        else
          basepath = '/'
          fname = path
          require 'open-uri'
          data = nil
          open(uri) { |f| read = f.read }
          ss = StringScanner.new(data)
          V1Parser.new(ss, basepath, fname, @eval_context).parse(true, nil, attrs, elems)
        end

      rescue SystemCallError => e
        cpe = ConfigParseError.new("include error #{uri}")
        cpe.set_backtrace(e.backtrace)
        raise cpe
      end

      # override
      def error_sample
        "#{@fname} #{super}"
      end
    end
  end
end
