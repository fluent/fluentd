#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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


class ConfigError < StandardError
end

class ConfigParseError < ConfigError
end


module Config
  class Element < Hash
    def initialize(name, arg, attrs, elements)
      @name = name
      @arg = arg
      @elements = elements
      super()
      attrs.each {|k,v|
        self[k] = v
      }
    end

    attr_accessor :name, :arg, :elements

    def add_element(name, arg='')
      e = Element.new(name, arg, {}, [])
      @elements << e
      e
    end

    def +(o)
      Element.new(@name.dup, @arg.dup, o.merge(self), @elements+o.elements)
    end

    def to_s(nest = 0)
      indent = "  "*nest
      out = ""
      if @arg.empty?
        out << "#{indent}<#{@name}>\n"
      else
        out << "#{indent}<#{@name} #{@name}>\n"
      end
      each_pair {|k,v|
        out << "#{indent}#{k} = #{v}\n"
      }
      @elements.each {|e|
        out << e.to_s(nest+1)
      }
      out << "#{indent}</#{@name}>\n"
      out
    end
  end

  def self.read(path)
    parse(File.read(path), File.basename(path))
  end

  def self.parse(str, fname)
    lines = str.split("\n")
    i, attrs, elems = parse_element('end', lines, 0, fname)
    Element.new('ROOT', '', attrs, elems)
  end

  def self.new(name='')
    Element.new('', '', {}, [])
  end

  def self.size_value(str)
    case str.to_s
    when /([0-9]+)k/i
      $~[1].to_i * 1024
    when /([0-9]+)m/i
      $~[1].to_i * (1024**2)
    when /([0-9]+)g/i
      $~[1].to_i * (1024**3)
    when /([0-9]+)t/i
      $~[1].to_i * (1024**4)
    else
      str.to_i
    end
  end

  def self.time_value(str)
    case str.to_s
    when /([0-9]+)s/
      $~[1].to_i
    when /([0-9]+)m/
      $~[1].to_i * 60
    when /([0-9]+)h/
      $~[1].to_i * 60*60
    else
      str.to_f
    end
  end

  private
  def self.parse_element(name, lines, i, fname)
    attrs = {}
    elems = []
    while i < lines.length
      line = lines[i]
      line.lstrip!
      line.gsub!(/\s*(?:\#.*)?$/,'')
        if line.empty?
          i += 1
          next
        elsif m = /^\<([a-zA-Z0-9_]+)\s*(.+?)?\>$/.match(line)
          e_name = m[1]
          e_arg = m[2] || ""
          i, e_attrs, e_elems = parse_element(e_name, lines, i+1, fname)
          elems << Element.new(e_name, e_arg, e_attrs, e_elems)
        elsif line == "</#{name}>"
          i += 1
          break
        elsif m = /^([a-zA-Z0-9_]+)\s*(.+)?$/.match(line)
          attrs[m[1]] = m[2] || ""
          i += 1
          next
        else
          raise ConfigParseError, "parse error at #{fname}:#{i}"
        end
    end
    return i, attrs, elems
  end
end


end

