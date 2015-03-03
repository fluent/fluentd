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

module Fluent
  module Plugin
    SEARCH_PATHS = []

    INPUT_REGISTRY = Registry.new(:input_type, 'fluent/plugin/in_')
    OUTPUT_REGISTRY = Registry.new(:output_type, 'fluent/plugin/out_')
    FILTER_REGISTRY = Registry.new(:filter_type, 'fluent/plugin/filter_')
    BUFFER_REGISTRY = Registry.new(:buffer_type, 'fluent/plugin/buf_')

    REGISTRIES = [INPUT_REGISTRY, OUTPUT_REGISTRY, FILTER_REGISTRY, BUFFER_REGISTRY]

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

    def self.register_parser(type, klass)
      TextParser.register_template(type, klass)
    end

    def self.register_formatter(type, klass)
      TextFormatter.register_template(type, klass)
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
      TextParser.lookup(type)
    end

    def self.new_formatter(type)
      TextFormatter.lookup(type)
    end

    def self.register_impl(name, registry, type, klass)
      registry.register(type, klass)
      $log.trace { "registered #{name} plugin '#{type}'" }
      nil
    end

    def self.new_impl(name, registry, type)
      if klass = registry.lookup(type)
        return klass.new
      end
      raise ConfigError, "Unknown #{name} plugin '#{type}'. Run 'gem search -rd fluent-plugin' to find plugins"
    end
  end
end
