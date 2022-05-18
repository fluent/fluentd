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

require 'psych'
require 'json'
require 'fluent/config/error'
require 'fluent/config/yaml_parser/fluent_value'

# Based on https://github.com/eagletmt/hako/blob/34cdde06fe8f3aeafd794be830180c3cedfbb4dc/lib/hako/yaml_loader.rb

module Fluent
  module Config
    module YamlParser
      class Loader
        INCLUDE_TAG = 'tag:include'.freeze
        FLUENT_JSON_TAG = 'tag:fluent/json'.freeze
        FLUENT_STR_TAG = 'tag:fluent/s'.freeze
        SHOVEL = '<<'.freeze

        def initialize(context = Kernel.binding)
          @context = context
          @current_path = nil
        end

        # @param [String] path
        # @return [Hash]
        def load(path)
          class_loader = Psych::ClassLoader.new
          scanner = Psych::ScalarScanner.new(class_loader)

          visitor = Visitor.new(scanner, class_loader)

          visitor._register_domain(INCLUDE_TAG) do |_, val|
            load(path.parent.join(val))
          end

          visitor._register_domain(FLUENT_JSON_TAG) do |_, val|
            Fluent::Config::YamlParser::FluentValue::JsonValue.new(val)
          end

          visitor._register_domain(FLUENT_STR_TAG) do |_, val|
            Fluent::Config::YamlParser::FluentValue::StringValue.new(val, @context)
          end

          path.open do |f|
            visitor.accept(Psych.parse(f))
          end
        end

        class Visitor < Psych::Visitors::ToRuby
          def initialize(scanner, class_loader)
            super(scanner, class_loader)
          end

          def _register_domain(name, &block)
            @domain_types.merge!({ name => [name, block] })
          end

          def revive_hash(hash, o)
            super(hash, o).tap do |r|
              if r[SHOVEL].is_a?(Hash)
                h2 = {}
                r.each do |k, v|
                  if k == SHOVEL
                    h2.merge!(v)
                  else
                    h2[k] = v
                  end
                end
                r.replace(h2)
              end
            end
          end
        end
      end
    end
  end
end
