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

    require 'fluent/config/basic_parser'
    require 'irb/ruby-lex'  # RubyLex

    class LiteralParser < BasicParser
      def initialize(strscan, eval_context)
        super(strscan)
        @eval_context = eval_context
      end

      def parse_literal(string_boundary_charset = LINE_END)
        spacing

        if skip(/true(?=#{string_boundary_charset})/)
          value = true

        elsif skip(/false(?=#{string_boundary_charset})/)
          value = false

        elsif skip(/NaN(?=#{string_boundary_charset})/)
          value = Float::NAN

        elsif skip(/Infinity(?=#{string_boundary_charset})/)
          value = Float::INFINITY

        elsif skip(/-Infinity(?=#{string_boundary_charset})/)
          value = -Float::INFINITY

        elsif m = scan(/\-?(?:0|[1-9][0-9]*)(?=#{string_boundary_charset})/)
          # integer
          value = m.to_i

        elsif m = scan(/\-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?=#{string_boundary_charset})/)
          # float
          value = m.to_f

        elsif skip(/\[/)
          value = cont_parse_array.to_json

        elsif skip(/\{/)
          value = cont_parse_map.to_json
        else
          value = scan_string(string_boundary_charset)
        end

        return value.to_s
      end

      def scan_string(string_boundary_charset = LINE_END)
        if skip(/\"/)
          return scan_quoted_string
        else
          return scan_nonquoted_string(string_boundary_charset)
        end
      end

      def scan_quoted_string
        string = []
        while true
          if skip(/\"/)
            return string.join
          elsif s = scan(/\\./)
            string << eval_escape_char(s[1,1])
          elsif skip(/\#\{/)
            string << eval_embedded_code(scan_embedded_code)
            skip(/\}/)
          elsif s = scan(/./)
            string << s
          else
            parse_error! "unexpected end of file in a quoted string"
          end
        end
      end

      def scan_nonquoted_string(boundary_charset = LINE_END)
        charset = /(?!#{boundary_charset})./

        string = []
        while true
          if s = scan(/\\./)
            string << eval_escape_char(s[1,1])
          elsif s = scan(charset)
            string << s
          else
            break
          end
        end

        if string.empty?
          return nil
        end

        string.join
      end

      def scan_embedded_code
        rlex = RubyLex.new
        src = '"#{'+@ss.rest+"\n=end\n}"

        input = StringIO.new(src)
        input.define_singleton_method(:encoding) { external_encoding }
        rlex.set_input(input)

        tk = rlex.token
        code = src[3,tk.seek-3]

        if @ss.rest.length < code.length
          @ss.pos += @ss.rest.length
          parse_error! "expected end of embedded code but $end"
        end

        @ss.pos += code.length

        code
      end

      def eval_embedded_code(code)
        if @eval_context.nil?
          parse_error! "embedded code is not allowed in this file"
        end
        @eval_context.instance_eval(code)
      end

      def eval_escape_char(c)
        case c
        when '"'
          '"'
        when "'"
          "'"
        when "r"
          "\r"
        when "n"
          "\n"
        when "t"
          "\t"
        when "f"
          "\f"
        when "b"
          "\b"
        when /[a-zA-Z0-9]/
          parse_error! "unexpected back-slash escape character '#{c}'"
        else  # symbols
          c
        end
      end

      def cont_parse_array
        spacing
        return [] if skip(/\]/)

        array = []
        while true
          array << parse_literal(/(?:#{SPACING}|[\]\,])/)

          spacing
          if skip(/\]/)
            return array
          end

          unless skip(/\,/)
            parse_error! "expected ',' or ']' in array"
          end

          # to allow last ','
          spacing
          return array if skip(/\]/)
        end
      end

      def cont_parse_map
        spacing
        return {} if skip(/\}/)

        map = {}
        while true
          key = scan_string(/(?:#{SPACING}|[\}\:\,])/)

          spacing
          unless skip(/\:/)
            parse_error! "expected ':' in map"
          end

          map[key] = parse_literal(/(?:#{SPACING}|[\}\,\:])/)

          spacing
          if skip(/\}/)
            return map
          end

          unless skip(/\,/)
            parse_error! "expected ',' or '}' in map"
          end

          # to allow last ','
          spacing
          return map if skip(/\}/)
        end
      end
    end
  end
end
