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

  require 'json'
  require 'fluentd/config_error'

  module Configurable
    def init_configurable
      self.class.config_defaults.each_pair {|name,defval|
        varname = :"@#{name}"
        instance_variable_set(varname, defval)
      }
    end

    def self.included(mod)
      mod.extend(ClassMethods)
    end

    def configure(conf)
      @config = conf

      self.class.config_params.each_pair {|name,(block,opts)|
        varname = :"@#{name}"
        if val = conf[name.to_s]
          val = self.instance_exec(val, opts, name, &block)
          instance_variable_set(varname, val)
        end
        unless instance_variable_defined?(varname)
          Engine.logger.error "config error in:\n#{conf}"
          raise ConfigError, "'#{name}' parameter is required"  # TODO config error
        end
      }
    end

    attr_reader :config

    CONFIG_TYPES = {}

    module ClassMethods
      def register_type(type, &block)
        CONFIG_TYPES[type] = block
      end

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

        begin
          block ||= Plugin.lookup_type(type)
        rescue ConfigError
          # override error message
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

    extend ClassMethods
  end

end
