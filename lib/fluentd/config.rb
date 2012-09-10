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

  require 'strscan'

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
    class Context
      # TODO
    end

    class ValueParser
      KEYWORDS = {
        /true/ => true,
        /false/ => false,
        /null/ => nil,
        /nil/ => nil,
        /NaN/ => Float::NAN,
        /Infinity/ => Float::INFINITY,
        /-Infinity/ => -Float::INFINITY,
      }

      INTEGER = /-?0|-?[1-9][0-9]*/
      FLOAT = /-?(?:0|[1-9][0-9]*)(?:(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)|\.[0-9]+)/
      MAP_KEY_CHARSET = /[a-zA-Z0-9_]+/
      SPECIAL_STRING_CHARSET = /[a-zA-Z0-9_\*\.\@]+/

      ARRAY_START = /\[/
      MAP_START = /\{/
      ARRAY_END = /\]/
      MAP_END = /\}/
      COMMA = /\,/
      COLON = /\:/

      LINE_END = /(?:[\r\n]|\#[\n]*)+/
      SPACING = /(?:\s|\#[\n]*)+/

      def initialize(ss, ruby_context)
        require 'irb/ruby-lex'
        require 'stringio'
        @ss = ss
        @ruby_context = ruby_context
      end

      def parse_value
        @ss.skip(SPACING)

        KEYWORDS.each {|k,v|
          if @ss.scan(k)
            return v
          end
        }
        if @ss.skip(ARRAY_START)
          return parse_array
        elsif @ss.skip(MAP_START)
          return parse_map
        elsif s = @ss.scan(FLOAT)
          return s.to_f
        elsif s = @ss.scan(INTEGER)
          return s.to_i
        elsif @ss.skip(/\"/)
          return parse_string
        end

        raise ConfigParseError, "expected character at '#{error_sample}'"
      end

      def parse_array
        @ss.skip(SPACING)
        if @ss.skip(ARRAY_END)
          return []
        end

        array = []

        while true
          e = parse_value
          array << e

          @ss.skip(SPACING)
          if @ss.skip(ARRAY_END)
            return array
          end

          @ss.skip(SPACING)
          unless @ss.skip(COMMA)
            raise ConfigParseError, "expected ',' or ']' in array at '#{error_sample}'"
          end

          # to allow last ','
          @ss.skip(SPACING)
          if @ss.skip(ARRAY_END)
            return array
          end
        end
      end

      def parse_map
        @ss.skip(SPACING)
        if @ss.skip(MAP_END)
          return []
        end

        map = {}

        while true
          k = parse_map_key

          @ss.skip(SPACING)
          unless @ss.skip(COLON)
            raise ConfigParseError, "expected ':' in map at '#{error_sample}'"
          end

          v = parse_value
          map[k] = v

          @ss.skip(SPACING)
          if @ss.skip(MAP_END)
            return map
          end

          @ss.skip(SPACING)
          unless @ss.skip(COMMA)
            raise ConfigParseError, "expected ',' or '}' in map at '#{error_sample}'"
          end

          # to allow last ','
          @ss.skip(SPACING)
          if @ss.skip(MAP_END)
            return map
          end
        end
      end

      def parse_string
        string = ''

        while true
          if @ss.skip(/"/)
            return string
          elsif s = @ss.scan(/(?:(?!"|\\|\$\{).)+/m)
            string << s
          elsif s = @ss.scan(/\\./)
            string << eval_escape_char(s[1,1])
          elsif @ss.skip(/\$\{/)
            string << parse_ruby_code
          else
            raise ConfigParseError, "expected character in quoted string at '#{error_sample}'"
          end
        end
      end

      def parse_map_key
        if @ss.skip(/\"/)
          return parse_string
        end

        string = ''

        while true
          if s = @ss.scan(MAP_KEY_CHARSET)
            string << s
          elsif s = @ss.scan(/\\./)
            string << eval_escape_char(s[1,1])
          elsif @ss.skip(/\$\{/)
            string << parse_ruby_code
          else
            if string.empty?
              raise ConfigParseError, "expected string at '#{error_sample}'"
            end
            return string
          end
        end
      end

      def parse_special_string
        if @ss.skip(/\"/)
          return parse_string
        end

        string = ''

        while true
          if s = @ss.scan(SPECIAL_STRING_CHARSET)
            string << s
          elsif s = @ss.scan(/\\./)
            string << eval_escape_char(s[1,1])
          elsif @ss.skip(/\$\{/)
            string << parse_ruby_code
          else
            return string
          end
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
          raise ConfigParseError, "expected escape string #{s} at '#{error_sample}'"
        else
          c
        end
      end

      def parse_ruby_code
        rlex = RubyLex.new
        src = '"#{'+@ss.rest
        input = StringIO.new(src)
        input.define_singleton_method(:encoding) { external_encoding }
        rlex.set_input(input)

        tk = rlex.token
        code = src[3,tk.seek-3]

        @ss.pos += code.length

        @ss.skip(/\s*\}/)

        eval_ruby_code(code).to_s
      end

      def eval_ruby_code(code)
        @ruby_context.instance_eval(code)
      end

      def error_sample
        @ss.peek(20)
      end
    end

    class Parser < ValueParser
      #SIMPLE_STRING = /(?![\{\[\"\'])[^\r\n\#]*(?=\s*\#(?:\r?\n)*)?/
      SIMPLE_STRING = /[^\r\n\#]*(?=\s*\#(?:\r?\n)*)?/

      def self.read(path, context=Context.new)
        path = File.expand_path(path)
        data = File.read(path)
        parse(data, File.basename(path), File.dirname(path), context)
      end

      def self.parse(data, fname, basepath=Dir.pwd, context=Context.new)
        ss = StringScanner.new(data)
        attrs, elems = Parser.new(ss, basepath, fname, context).parse(true)
        Element.new('ROOT', '', attrs, elems)
      end

      def initialize(ss, basepath, fname, context, line=0)
        super(ss, context)
        @basepath = basepath
        @line = line
        @fname = fname
      end

      def parse(allow_include, elem_name=nil, attrs={}, elems=[])
        while true
          @ss.skip(SPACING)
          break if @ss.eos?

          if @ss.skip(/\<\//)
            name = parse_map_key
            unless @ss.skip(/[ \t]*\>/)
              raise ConfigParseError, "expected > at #{error_sample}"
            end
            if name != elem_name
              raise ConfigParseError, "unmatched end tag string at #{error_sample}"
            end
            break

          elsif @ss.skip(/\</)
            e_name = parse_map_key
            unless @ss.skip(/[ \t]*\>/)
              @ss.skip(/[ \t]*/)
              e_arg = parse_special_string
              unless @ss.skip(/[ \t]*\>/)
                raise ConfigParseError, "expected > at #{error_sample}"
              end
            end
            e_arg ||= ''  # FIXME
            e_attrs, e_elems = parse(false, e_name)
            elems << Element.new(e_name, e_arg, e_attrs, e_elems)

          else
            k = parse_map_key
            if @ss.skip(/[ \t]*:/)
              if @ss.skip(LINE_END)
                v = nil
              else
                v = parse_value
              end
            elsif @ss.skip(/[ \t]+/)
              # backward compatibility
              if @ss.skip(LINE_END)
                v = ""
              else
                v = parse_simple_string
              end
            else
              v = ""
            end

            if allow_include && k == 'include'
              process_include(attrs, elems, value)
            else
              attrs[k] = v
            end

          end
        end

        return attrs, elems
      end

      # override
      def parse_value
        begin
          pos = @ss.pos
          begin
            v = super
          rescue ConfigParseError => e
            # backward compatibility
            begin
              @ss.pos = pos
              v = try_parse_simple_string
              unless v
                raise ConfigParseError, "expected \\n or EOF in array at '#{error_sample}'"
              end
            rescue
              raise e
            end
          end

          @ss.skip(SPACING)
          unless @ss.eos? || @ss.string[@ss.pos-1] == "\n"
            raise ConfigParseError, "expected \\n or EOF in array at '#{error_sample}'"
          end

          pos = nil
          return v

        ensure
          @pos = pos if pos
        end
      end

      def parse_simple_string
        if s = try_parse_simple_string
          return s
        end
        raise ConfigParseError, "unexpected character at '#{error_sample}'"
      end

      def try_parse_simple_string
        pos = @ss.pos
        if s = @ss.scan(SIMPLE_STRING)
          if @ss.skip(LINE_END)
            return s.rstrip
          end
          @ss.pos = pos
        end
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
            data = File.read(path)
            ss = StringScanner.new(data)
            Parser.new(ss, basepath, fname, @ruby_context).parse(true, nil, attrs, elems)
          }

        else
          basepath = '/'
          fname = path
          require 'open-uri'
          data = nil
          open(uri) {|f| read = f.read }
          ss = StringScanner.new(data)
          Parser.new(ss, basepath, fname, @ruby_context).parse(true, nil, attrs, elems)
        end

      rescue SystemCallError
        e = ConfigParseError.new("include error #{uri}")
        e.set_backtrace($!.backtrace)
        raise e
      end
    end
  end

end

