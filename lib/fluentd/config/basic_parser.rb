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
    require 'fluentd/config_error'

    BASIC_CHARACTERS = /[a-zA-Z0-9_\/\.\-\+\*\@\=]/

    module ParserModule
      BOUNDARY = /(?!#{BASIC_CHARACTERS})/

      def keyword(string)
        /#{Regexp.escape(string)}#{BOUNDARY}/
      end

      def symbol(string)
        /#{Regexp.escape(string)}/
      end

      def token(pattern)
        /#{pattern}#{BOUNDARY}/
      end

      def def_keyword(name, string=name.to_s)
        pattern = keyword(string)
        define_method(name) do
          skip(pattern) && string
        end
      end

      def def_symbol(name, string=name.to_s)
        pattern = symbol(string)
        define_method(name) do
          skip(pattern) && string
        end
      end

      def def_token(name, pattern)
        pattern = token(pattern)
        define_method(name) do
          scan(pattern)
        end
      end
    end

    class BasicParser
      SPACING = /(?:[ \t\r\n]|\z|\#.*?(?:\z|[\r\n]))+/

      extend ParserModule

      def initialize(strscan)
        @ss = strscan
      end

      def skip(pattern)
        @ss.skip(pattern)
      end

      def scan(pattern)
        @ss.scan(pattern)
      end

      def eof?
        @ss.eos?
      end

      def spacing
        skip(/(?:[ \t\r\n]|\z|\#.*?(?:\z|[\r\n]))+/)
      end

      def parse_error!(message)
        raise ConfigParseError, "#{message} at #{error_sample}"
      end

      def error_sample
        pos = @ss.pos

        lines = @ss.string.lines.to_a
        lines.each_with_index {|line,ln|
          if line.size > pos
            msgs = ["line #{ln+1},#{pos}\n"]

            if ln > 0
              last_line = lines[ln-1]
              msgs << "%3s: %s" % [ln, last_line]
            end

            msgs << "%3s: %s" % [ln+1, line]
            msgs << "     #{'-'*pos}^\n"

            if next_line = lines[ln+1]
              msgs << "%3s: %s" % [ln+2, next_line]
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
