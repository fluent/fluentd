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

require 'stringio'
require 'fluent/config/error'

module Fluent
  module Config
    class BasicParser
      def initialize(strscan)
        @ss = strscan
      end

      LINE_END = /(?:[ \t]*(?:\#.*)?(?:\z|[\r\n]))+/
      SPACING = /(?:[ \t\r\n]|\z|\#.*?(?:\z|[\r\n]))+/
      ZERO_OR_MORE_SPACING = /(?:[ \t\r\n]|\z|\#.*?(?:\z|[\r\n]))*/
      SPACING_WITHOUT_COMMENT = /(?:[ \t\r\n]|\z)+/
      LINE_END_WITHOUT_SPACING_AND_COMMENT = /(?:\z|[\r\n])/

      module ClassMethods
        def symbol(string)
          /#{Regexp.escape(string)}/
        end

        def def_symbol(method_name, string)
          pattern = symbol(string)
          define_method(method_name) do
            skip(pattern) && string
          end
        end

        def def_literal(method_name, string)
          pattern = /#{string}#{LINE_END}/
          define_method(method_name) do
            skip(pattern) && string
          end
        end
      end

      extend ClassMethods

      def skip(pattern)
        @ss.skip(pattern)
      end

      def scan(pattern)
        @ss.scan(pattern)
      end

      def getch
        @ss.getch
      end

      def eof?
        @ss.eos?
      end

      def prev_match
        @ss[0]
      end

      def check(pattern)
        @ss.check(pattern)
      end

      def line_end
        skip(LINE_END)
      end

      def spacing
        skip(SPACING)
      end

      def spacing_without_comment
        skip(SPACING_WITHOUT_COMMENT)
      end

      def parse_error!(message)
        raise ConfigParseError, "#{message} at #{error_sample}"
      end

      def error_sample
        pos = @ss.pos

        lines = @ss.string.lines.to_a
        lines.each_with_index { |line, ln|
          if line.size >= pos
            msgs = ["line #{ln + 1},#{pos}\n"]

            if ln > 0
              last_line = lines[ln - 1]
              msgs << "%3s: %s" % [ln, last_line]
            end

            msgs << "%3s: %s" % [ln + 1, line]
            msgs << "\n     #{'-' * pos}^\n"

            if next_line = lines[ln + 1]
              msgs << "%3s: %s" % [ln + 2, next_line]
            end

            return msgs.join
          end
          pos -= line.size
          last_line = line
        }
      end
    end
  end
end
