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

  def self.read(path)
    Parser.read(path)
  end

  def self.parse(str, fname, basepath=Dir.pwd)
    Parser.parse(str, fname, basepath)
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
    when /([0-9]+)d/
      $~[1].to_i * 24*60*60
    else
      str.to_f
    end
  end

  def self.bool_value(str)
    case str.to_s
    when 'true', 'yes'
      true
    when 'false', 'no'
      false
    else
      nil
    end
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


module Configurable
  def self.included(mod)
    mod.extend(ClassMethods)
  end

  def initialize
    self.class.config_defaults.each_pair {|name,defval|
      varname = :"@#{name}"
      instance_variable_set(varname, defval)
    }
  end

  def configure(conf)
    self.class.config_params.each_pair {|name,(block,opts)|
      varname = :"@#{name}"
      if val = conf[name.to_s]
        val = self.instance_exec(val, opts, name, &block)
        instance_variable_set(varname, val)
      end
      unless instance_variable_defined?(varname)
        $log.error "config error in:\n#{conf}"
        raise ConfigError, "'#{name}' parameter is required"
      end
    }
  end

  module ClassMethods
    def config_param(name, *args, &block)
      name = name.to_sym

      opts = {}
      args.each {|a|
        if a.is_a?(Symbol)
          opts[:type] = a
        elsif a.is_a?(Hash)
          opts.merge!(a)
        else
          raise ArgumentError, "wrong number of arguments (#{1+args.length} for #{block ? 2 : 3})"
        end
      }

      type = opts[:type]
      if block && type
        raise ArgumentError, "wrong number of arguments (#{1+args.length} for #{block ? 2 : 3})"
      end

      block ||= case type
          when :string, nil
            Proc.new {|val| val }
          when :integer
            Proc.new {|val| val.to_i }
          when :float
            Proc.new {|val| val.to_f }
          when :size
            Proc.new {|val| Config.size_value(val) }
          when :bool
            Proc.new {|val| Config.bool_value(val) }
          when :time
            Proc.new {|val| Config.time_value(val) }
          else
            raise ArgumentError, "unknown config_param type `#{type}'"
          end

      params = config_params_set
      params.delete(name)
      params[name] = [block, opts]

      if opts.has_key?(:default)
        config_set_default(name, opts[:default])
      end

      attr_accessor name
    end

    def config_set_default(name, defval)
      name = name.to_sym

      defaults = config_defaults_set
      defaults.delete(name)
      defaults[name] = defval

      nil
    end

    def config_params
      singleton_value(:_config_params)
    end

    def config_defaults
      singleton_value(:_config_defaults)
    end

    private
    def config_params_set
      singleton_value_set(:_config_params)
    end

    def config_defaults_set
      singleton_value_set(:_config_defaults)
    end

    def singleton_value_set(name)
      if methods(false).include?(name)
        __send__(name)
      else
        val = {}
        define_singleton_method(name) { val }
        val
      end
    end

    def singleton_value(name)
      val = {}
      ancestors.reverse_each {|c|
        if c.methods(false).include?(name)
          val.merge!(c.__send__(name))
        end
      }
      val
    end
  end
end


end

