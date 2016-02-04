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
    #   ex: storage, buffer(chunk), ...

    INPUT_REGISTRY = Registry.new(:input_type, 'fluent/plugin/in_')
    OUTPUT_REGISTRY = Registry.new(:output_type, 'fluent/plugin/out_')
    FILTER_REGISTRY = Registry.new(:filter_type, 'fluent/plugin/filter_')
    BUFFER_REGISTRY = Registry.new(:buffer_type, 'fluent/plugin/buf_')
    PARSER_REGISTRY = Registry.new(:config_type, 'fluent/plugin/parser_')
    FORMATTER_REGISTRY = Registry.new(:formatter_type, 'fluent/plugin/formatter_')
    STORAGE_REGISTRY = Registry.new(:storage, 'fluent/plugin/storage/') # storage for plugins

    REGISTRIES = [INPUT_REGISTRY, OUTPUT_REGISTRY, FILTER_REGISTRY, BUFFER_REGISTRY, PARSER_REGISTRY, FORMATTER_REGISTRY, STORAGE_REGISTRY]

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
      register_impl('parser', PARSER_REGISTRY, type, klass_or_proc)
    end

    def self.register_formatter(type, klass_or_proc)
      register_impl('formatter', FORMATTER_REGISTRY, type, klass_or_proc)
    end

    def self.register_storage(type, klass)
      register_impl('storage', STORAGE_REGISTRY, type, klass)
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

    def self.new_buffer(type)
      new_impl('buffer', BUFFER_REGISTRY, type)
    end

    def self.new_parser(type)
      new_impl('parser', PARSER_REGISTRY, type)
    end

    def self.new_formatter(type)
      new_impl('formatter', FORMATTER_REGISTRY, type)
    end

    def self.new_storage(type)
      new_impl('storage', STORAGE_REGISTRY, type)
    end

    def self.register_impl(name, registry, type, klass)
      registry.register(type, klass)
      $log.trace { "registered #{name} plugin '#{type}'" }
      nil
    end

    def self.new_impl(name, registry, type)
      obj = registry.lookup(type)
      case
      when obj.is_a?(Class)
        obj.new
      when obj.respond_to?(:call)
        obj.call
      else
        raise ConfigError, "Unknown #{name} plugin '#{type}'. Run 'gem search -rd fluent-plugin' to find plugins"
      end
    end
  end
end
