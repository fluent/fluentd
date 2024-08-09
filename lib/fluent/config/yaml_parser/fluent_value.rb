# frozen_string_literal: true

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
  module Config
    module YamlParser
      module FluentValue
        JsonValue = Struct.new(:val) do
          def to_s
            val.to_json
          end

          def to_element
            to_s
          end
        end

        StringValue = Struct.new(:val, :context) do
          def to_s
            context.instance_eval("\"#{val}\"").freeze
          rescue Fluent::SetNil => _
            ''
          rescue Fluent::SetDefault => _
            ':default'
          end

          def to_element
            to_s
          end
        end
      end
    end
  end
end
