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

  module Configurable
    def self.included(mod)
      mod.extend(ClassMethods)
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
        if (block && type) || (!block && !type)
          raise ArgumentError, "wrong number of arguments (#{1+args.length} for #{block ? 2 : 3})"
        end

        block ||= ConfigTypeSchema.new.build(type, opts)

        params = config_params_set
        params.delete(name)
        params[name] = [block, opts]

        if opts.has_key?(:default)
          # TODO
          #config_set_default(name, opts[:default])
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
        recursive_singleton_hash_merge(:_config_params)
      end

      def config_defaults
        recursive_singleton_hash_merge(:_config_defaults)
      end

      private

      def config_params_set
        singleton_hash_set(:_config_params)
      end

      def config_defaults_set
        singleton_hash_set(:_config_defaults)
      end

      def singleton_hash_set(name)
        if methods(false).include?(name)
          __send__(name)
        else
          val = {}
          define_singleton_method(name) { val }
          val
        end
      end

      def recursive_singleton_hash_merge(name)
        val = {}
        ancestors.reverse_each {|c|
          if c.methods(false).include?(name)
            val.merge!(c.__send__(name))
          end
        }
        val
      end
    end

    def initialize
      self.class.config_defaults.each_pair {|name,defval|
        varname = :"@#{name}"
        instance_variable_set(varname, defval)
      }
      super
    end

    def configure(conf)
      self.class.config_params.each_pair {|name,(block,opts)|
        varname = :"@#{name}"
        if val = conf[name.to_s]
          val = self.instance_exec(val, opts, name, &block)
          instance_variable_set(varname, val)
        end
        unless instance_variable_defined?(varname)
          conf.log.error "config error in:\n#{conf}"
          raise ConfigError, "'#{name}' parameter is required"
        end
      }
    end
  end

  class ConfigTypeSchema
    # TODO add converters for array<T>, hash<K,V>, any, etc.
    def build(type, opts)
      m = self
      case type
      when :string, nil
        Proc.new {|val| m.convert_string(val, opts) }
      when :integer
        Proc.new {|val| m.convert_integer(val, opts) }
      when :float
        Proc.new {|val| m.convert_float(val, opts) }
      when :size
        Proc.new {|val| m.convert_size(val, opts) }
      when :bool
        Proc.new {|val| m.convert_bool(val, opts) }
      when :time
        Proc.new {|val| m.convert_time(val, opts) }
      when :hash
        Proc.new {|val| m.convert_hash(val, opts) }
      else
        raise ArgumentError, "unknown config_param type `#{type}'"
      end
    end

    def convert_string(val, opts)
      val.to_s
    end

    def convert_integer(val, opts)
      val.to_i
    end

    def convert_float(val, opts)
      val.to_f
    end

    def convert_size(val, opts)
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

    def convert_bool(val, opts)
      case str.to_s
      when 'true', 'yes'
        true
      when 'false', 'no'
        false
      else
        nil
      end
    end

    def convert_time(val, opts)
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

    def convert_hash(val, opts)
      case val
      when Hash
        val
      when String
        JSON.load(val)
      else
        raise ConfigError, "hash required but got #{val.inspect}"
      end
    end
  end
end
