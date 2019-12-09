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

require 'forwardable'

module Fluent
  # VariableStore provides all pluigns with the way to shared variable without using class variable
  # it's for safe reloading mechanism
  class VariableStore
    @data = {}

    class << self
      def fetch_or_build(namespace, default_value: {})
        @data[namespace] ||= default_value
      end

      def try_to_reset
        @data, old = {}, @data

        begin
          yield
        rescue
          @data = old
          raise
        end
      end
    end
  end
end
