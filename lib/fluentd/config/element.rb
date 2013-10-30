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

      def new_element(name, arg)
        e = clone  # clone copies singleton methods
        e.instance_eval do
          clear
          @name = name
          @arg = arg
          @elements = []
          @used = []
        end
        e
      end

      def add_element(name, arg='')
        e = Element.new(name, arg, {}, [])
        @elements << e
        e
      end

      def +(o)
        Element.new(@name.dup, @arg.dup, o.merge(self), @elements+o.elements, @used+o.used)
      end

      def each_element(*names, &block)
        if names.empty?
          @elements.each(&block)
        else
          @elements.each {|e|
            if names.include?(e.name)
              block.yield(e)
            end
          }
        end
      end

      def has_key?(key)
        @used << key
        super
      end

      def [](key)
        @used << key
        super
      end

      def check_not_used(&block)
        each_key {|key|
          unless @used.include?(key)
            block.call(key, self)
          end
        }
        @elements.each {|e|
          e.check_not_used(&block)
        }
      end

      def to_s(nest=0)
        unless nest
          disable_recursive = true
          nest = 0
        end

        indent = "  "*nest
        nindent = "  "*(nest+1)
        out = ""
        if @arg.empty?
          out << "#{indent}<#{@name}>\n"
        else
          out << "#{indent}<#{@name} #{@arg}>\n"
        end

        nonquote_regexp = /[a-zA-Z0-9_]+/

        each_pair {|k,v|
          a = (k =~ nonquote_regexp) ? k : {"_"=>k}.to_json[5..-2]
          #b = (k =~ nonquote_regexp) ? v : {"_"=>v}.to_json[5..-2]
          b = {"_"=>v}.to_json[5..-2]
          out << "#{nindent}#{a} #{b}\n"
        }

        if disable_recursive
          unless @elements.empty?
            out << "#{nindent}...\n"
          end
        else
          @elements.each {|e|
            out << e.to_s(nest+1)
          }
        end

        out << "#{indent}</#{@name}>\n"
        out
      end

      def inspect
        to_s
      end
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

  end
end
