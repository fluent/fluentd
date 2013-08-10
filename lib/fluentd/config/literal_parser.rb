#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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

    require 'stringio'
    require_relative 'basic_parser'

    require 'irb/ruby-lex'  # RubyLex

    class LiteralParser < BasicParser
      def_keyword :k_true, "true"
      def_keyword :k_false, "false"
      def_keyword :k_nil, "nil"
      def_keyword :k_nan, "NaN"
      def_keyword :k_infinity, "Infinity"
      def_keyword :k_minus_infinity, "-Infinity"

      def_symbol :k_array_start, '['
      def_symbol :k_array_end, ']'
      def_symbol :k_map_start, '{'
      def_symbol :k_map_end, '}'

      def_symbol :k_comma, ','
      def_symbol :k_colon, ':'

      def_symbol :k_double_quote, '"'
      def_symbol :k_single_quote, '"'

      def_symbol :k_embedded_code_start, "${"
      STRING_EMBEDDED_CODE_START = /\$\{/
      EMBEDDED_CODE_END = /(?:\s|\#.*?)*\}/

      NONQUOTED_STRING_CHARSET = /[a-zA-Z0-9_]/
      NONQUOTED_STRING_FIRST_CHARSET = /[a-zA-Z_]/

      MAP_KEY_STRING_CHARSET = NONQUOTED_STRING_CHARSET

      def_token :tok_integer, /-?0|-?[1-9][0-9]*/
      def_token :tok_float, /-?(?:0|[1-9][0-9]*)(?:(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)|\.[0-9]+)/

      def parse_literal
        spacing

        if k_true
          true
        elsif k_false
          false
        elsif k_nil
          nil
        elsif k_nan
          Float::NAN
        elsif k_infinity
          Float::INFINITY
        elsif k_minus_infinity
          -Float::INFINITY
        elsif k_array_start
          return cont_parse_array
        elsif k_map_start
          return cont_parse_map
        elsif s = tok_float
          s.to_f
        elsif s = tok_integer
          s.to_i
        elsif k_embedded_code_start
          eval_embedded_code(cont_parse_embedded_code)
        else
          parse_string
        end
      end

      def parse_string
        spacing

        if string = try_parse_quoted_string
          return string
        end

        string = try_parse_special_string(NONQUOTED_STRING_CHARSET, NONQUOTED_STRING_FIRST_CHARSET)
        unless string
          parse_error! "expected string"
        end

        return string
      end

      def parse_map_key_string
        spacing

        if string = try_parse_quoted_string
          return string
        end

        string = try_parse_special_string(MAP_KEY_STRING_CHARSET)
        unless string
          parse_error! "expected key name"
        end

        return string
      end

      def try_parse_quoted_string
        if k_double_quote
          return cont_parse_double_quoted_string
        end

        # TODO single quoted string

        return nil
      end

      def self.nonquoted_string?(v)
        v.is_a?(String) && v[0] =~ NONQUOTED_STRING_FIRST_CHARSET && v[1..-1] =~ /\A#{NONQUOTED_STRING_CHARSET}*\z/
      end

      def cont_parse_double_quoted_string
        string = ''

        while true
          if skip(/"/)
            return string
          elsif s = scan(/(?:(?!"|\\|\$\{).)+/m)
            string << s
          elsif s = scan(/\\./)
            string << eval_escape_char(s[1,1])
          elsif skip(STRING_EMBEDDED_CODE_START)
            string << eval_embedded_code(cont_parse_embedded_code).to_s
          else
            parse_error! "unexpected character in quoted string"
          end
        end
      end

      def try_parse_special_string(charset, first_char_charset=charset)
        string = scan(first_char_charset)
        return nil unless string

        while true
          if s = scan(charset)
            string << s
          elsif s = scan(/\\./)
            string << eval_escape_char(s[1,1])
          elsif skip(STRING_EMBEDDED_CODE_START)
            string << eval_embedded_code(cont_parse_embedded_code).to_s
          else
            return string
          end
        end
      end

      def eval_embedded_code(code)
        parse_error! "embedded code is not supported"
      end

      def cont_parse_array
        spacing
        return [] if k_array_end

        array = []
        while true
          array << parse_literal

          spacing
          return array if k_array_end

          spacing
          unless k_comma
            parse_error! "expected ',' or ']' in array"
          end

          # to allow last ','
          spacing
          return array if k_array_end
        end
      end

      def cont_parse_map
        spacing
        return {} if k_map_end

        map = {}
        while true
          key = parse_string

          spacing
          unless k_colon
            parse_error! "expected ':' in map"
          end

          map[key] = parse_literal

          spacing
          return map if k_map_end

          spacing
          unless k_comma
            parse_error! "expected ',' or '}' in map"
          end

          # to allow last ','
          spacing
          return map if k_map_end
        end
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
          parse_error! "unexpected escape string '#{c}'"
        else
          c
        end
      end

      def cont_parse_embedded_code
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

        skip(EMBEDDED_CODE_END)

        code
      end
    end

  end
end
