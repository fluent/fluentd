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

require 'fluent/registry'
require 'fluent/config/error'

module Fluent
  module Plugin
    SEARCH_PATHS = []

    # plugins for fluentd:         fluent/plugin/type_NAME.rb
    # plugins for fluentd plugins: fluent/plugin/type/NAME.rb
    #   ex: storage, buffer chunk, ...

    # first class plugins (instantiated by Engine)
    INPUT_REGISTRY     = Registry.new(:input,     'fluent/plugin/in_',         dir_search_prefix: 'in_')
    OUTPUT_REGISTRY    = Registry.new(:output,    'fluent/plugin/out_',        dir_search_prefix: 'out_')
    FILTER_REGISTRY    = Registry.new(:filter,    'fluent/plugin/filter_',     dir_search_prefix: 'filter_')

    # feature plugin: second class plugins (instantiated by Plugins or Helpers)
    BUFFER_REGISTRY    = Registry.new(:buffer,    'fluent/plugin/buf_',        dir_search_prefix: 'buf_')
    PARSER_REGISTRY    = Registry.new(:parser,    'fluent/plugin/parser_',     dir_search_prefix: 'parser_')
    FORMATTER_REGISTRY = Registry.new(:formatter, 'fluent/plugin/formatter_',  dir_search_prefix: 'formatter_')
    STORAGE_REGISTRY   = Registry.new(:storage,   'fluent/plugin/storage_',    dir_search_prefix: 'storage_')

    REGISTRIES = [INPUT_REGISTRY, OUTPUT_REGISTRY, FILTER_REGISTRY, BUFFER_REGISTRY, PARSER_REGISTRY, FORMATTER_REGISTRY, STORAGE_REGISTRY]

    def self.register_input(type, klass)
      register_impl('input', INPUT_REGISTRY, type, klass)
    end

    def self.register_output(type, klass)
      register_impl('output', OUTPUT_REGISTRY, type, klass)
    end

    def self.register_filter(type, klass)
      register_impl('filter', FILTER_REGISTRY, type, klass)
    end

    def self.register_buffer(type, klass)
      register_impl('buffer', BUFFER_REGISTRY, type, klass)
    end

    def self.register_parser(type, klass_or_proc)
      if klass_or_proc.is_a?(Regexp)
        # This usage is not recommended for new API
        require 'fluent/parser'
        register_impl('parser', PARSER_REGISTRY, type, Proc.new { Fluent::TextParser::RegexpParser.new(klass_or_proc) })
      else
        register_impl('parser', PARSER_REGISTRY, type, klass_or_proc)
      end
    end

    def self.register_formatter(type, klass_or_proc)
      if klass_or_proc.respond_to?(:call) && klass_or_proc.arity == 3 # Proc.new { |tag, time, record| }
        # This usage is not recommended for new API
        require 'fluent/formatter'
        register_impl('formatter', FORMATTER_REGISTRY, type, Proc.new { Fluent::TextFormatter::ProcWrappedFormatter.new(klass_or_proc) })
      else
        register_impl('formatter', FORMATTER_REGISTRY, type, klass_or_proc)
      end
    end

    def self.register_storage(type, klass)
      register_impl('storage', STORAGE_REGISTRY, type, klass)
    end

    def self.lookup_type_from_class(klass_or_its_name)
      klass = if klass_or_its_name.is_a? Class
                klass_or_its_name
              elsif klass_or_its_name.is_a? String
                eval(klass_or_its_name) # const_get can't handle qualified klass name (ex: A::B)
              else
                raise ArgumentError, "invalid argument type #{klass_or_its_name.class}: #{klass_or_its_name}"
              end
      REGISTRIES.reduce(nil){|a, r| a || r.reverse_lookup(klass) }
    end

    def self.add_plugin_dir(dir)
      REGISTRIES.each do |r|
        r.paths.push(dir)
      end
      nil
    end

    def self.new_input(type)
      new_impl('input', INPUT_REGISTRY, type)
    end

    def self.new_output(type)
      new_impl('output', OUTPUT_REGISTRY, type)
    end

    def self.new_filter(type)
      new_impl('filter', FILTER_REGISTRY, type)
    end

    def self.new_buffer(type, parent: nil)
      new_impl('buffer', BUFFER_REGISTRY, type, parent)
    end

    def self.new_parser(type, parent: nil)
      if type[0] == '/' && type[-1] == '/'
        # This usage is not recommended for new API... create RegexpParser directly
        require 'fluent/parser'
        impl = Fluent::TextParser.lookup(type)
        impl.extend FeatureAvailabilityChecker
        impl
      else
        new_impl('parser', PARSER_REGISTRY, type, parent)
      end
    end

    def self.new_formatter(type, parent: nil)
      new_impl('formatter', FORMATTER_REGISTRY, type, parent)
    end

    def self.new_storage(type, parent: nil)
      new_impl('storage', STORAGE_REGISTRY, type, parent)
    end

    def self.register_impl(kind, registry, type, value)
      if !value.is_a?(Class) && !value.respond_to?(:call)
        raise Fluent::ConfigError, "Invalid implementation as #{kind} plugin: '#{type}'. It must be a Class, or callable."
      end
      registry.register(type, value)
      $log.trace "registered #{kind} plugin '#{type}'" if defined?($log)
      nil
    end

    def self.new_impl(kind, registry, type, parent=nil)
      # "'type' not found" is handled by registry
      obj = registry.lookup(type)
      impl = case
             when obj.is_a?(Class)
               obj.new
             when obj.respond_to?(:call) && obj.arity == 0
               obj.call
             else
               raise Fluent::ConfigError, "#{kind} plugin '#{type}' is not a Class nor callable (without arguments)."
             end
      if parent && impl.respond_to?("owner=")
        impl.owner = parent
      end
      impl.extend FeatureAvailabilityChecker
      impl
    end

    module FeatureAvailabilityChecker
      def configure(conf)
        super

        # extend plugin instance by this module
        # to run this check after all #configure methods of plugins and plugin helpers
        sysconf = if self.respond_to?(:owner) && owner.respond_to?(:system_config)
                    owner.system_config
                  elsif self.respond_to?(:system_config)
                    self.system_config
                  else
                    nil
                  end

        if sysconf && sysconf.workers > 1 && !self.multi_workers_ready?
          type = Fluent::Plugin.lookup_type_from_class(self.class)
          raise Fluent::ConfigError, "Plugin '#{type}' does not support multi workers configuration (#{self.class})"
        end
      end
    end
  end
end
