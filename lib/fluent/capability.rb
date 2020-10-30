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
  class Capability
    def initialize(target = :current_process, pid_or_path = nil)
      if defined?(CapNG)
        @usable = true
        @capng = CapNG.new(target, pid_or_path)
      else
        @usable = false
      end
    end

    def usable?
      @usable
    end

    def apply(select_set)
      return nil unless usable?

      @capng.apply(select_set)
    end

    def clear(select_set)
      return nil unless usable?

      @capng.clear(select_set)
    end

    def have_capability?(type, capability)
      return false unless usable?

      @capng.have_capability?(type, capability)
    end

    def update(action, type, capability_or_capability_array)
      return nil unless usable?

      @capng.update(action, type, capability_or_capability_array)
    end

    def have_capabilities?(select_set)
      return false unless usable?

      @capng.have_capabilities?(select_set)
    end
  end
end
