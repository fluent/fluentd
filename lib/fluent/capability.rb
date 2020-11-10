#
# Fluent
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

require "fluent/env"

if Fluent.linux?
  begin
    require 'capng'
  rescue LoadError
  end
end

module Fluent
  if defined?(CapNG)
    class Capability
      def initialize(target = nil, pid = nil)
        @capng = CapNG.new(target, pid)
      end

      def usable?
        true
      end

      def apply(select_set)
        @capng.apply(select_set)
      end

      def clear(select_set)
        @capng.clear(select_set)
      end

      def have_capability?(type, capability)
        @capng.have_capability?(type, capability)
      end

      def update(action, type, capability_or_capability_array)
        @capng.update(action, type, capability_or_capability_array)
      end

      def have_capabilities?(select_set)
        @capng.have_capabilities?(select_set)
      end
    end
  else
    class Capability
      def initialize(target = nil, pid = nil)
      end

      def usable?
        false
      end

      def apply(select_set)
        false
      end

      def clear(select_set)
        false
      end

      def have_capability?(type, capability)
        false
      end

      def update(action, type, capability_or_capability_array)
        false
      end

      def have_capabilities?(select_set)
        false
      end
    end
  end
end
