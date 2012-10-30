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

        if (block && type) || (!block && !type)
          raise ArgumentError, "wrong number of arguments (#{1+args.length} for #{block ? 2 : 3})"
        end

        block ||= ConfigTypes.new_converter(type, opts)

        params = config_params_set
        params.delete(name)
        params[name] = [block, opts]

        if opts.has_key?(:default)
          config_set_default(name, opts[:default])
        end

        attr_accessor name
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
          $log.error "config error in:\n#{conf}"
          raise ConfigError, "'#{name}' parameter is required"
        end
      }
    end
  end

  module ConfigTypes
    # TODO add converters for array<T>, hash<K,V>, any, etc.
    def self.new_converter(type, opts)
      case type
      when :string, nil
        lambda {|val| convert_string(val, opts) }
      when :integer
        lambda {|val| convert_integer(val, opts) }
      when :float
        lambda {|val| convert_float(val, opts) }
      when :size
        lambda {|val| convert_size(val, opts) }
      when :bool
        lambda {|val| convert_bool(val, opts) }
      when :time
        lambda {|val| convert_time(val, opts) }
      else
        raise ArgumentError, "unknown config_param type `#{type}'"
      end
    end

    def self.convert_string(val, opts)
      val.to_s
    end

    def self.convert_integer(val, opts)
      val.to_i
    end

    def self.convert_float(val, opts)
      val.to_i
    end

    def self.convert_size(val, opts)
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

    def self.convert_bool(val, opts)
      case str.to_s
      when 'true', 'yes'
        true
      when 'false', 'no'
        false
      else
        nil
      end
    end

    def self.convert_time(val, opts)
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
  end
end
