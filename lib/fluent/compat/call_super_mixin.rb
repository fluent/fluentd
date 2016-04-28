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
  module Compat
    module CallSuperMixin
      # This mixin is to prepend to 3rd party plugins of v0.12 APIs.
      # In past, there were not strong rule to call super in #start, #before_shutdown and #shutdown.
      # But v0.14 API requires to call super in these methods to setup/teardown plugin helpers and others.
      # This mixin prepends method calls to call super forcedly if checker returns false (it shows Fluent::Plugin::Base#methods wasn't called)

      def self.prepended(klass)
        @@_super_start ||= {}
        @@_super_before_shutdown ||= {}
        @@_super_shutdown ||= {}

        # ancestors[0]: this module
        # ancestors[1]: prepended class (plugin itself)
        method_search = ->(ancestors, method){
          closest = ancestors[2, ancestors.size - 2].index{|m| m.method_defined?(method) }
          ancestors[2 + closest].instance_method(method)
        }
        @@_super_start[klass]           = method_search.call(klass.ancestors, :start) # this returns Fluent::Compat::*#start (or helpers on it)
        @@_super_before_shutdown[klass] = method_search.call(klass.ancestors, :before_shutdown)
        @@_super_shutdown[klass]        = method_search.call(klass.ancestors, :shutdown)
      end

      def start
        super
        unless self.started?
          @@_super_start[self.class].bind(self).call
          # #super will reset logdev (especially in test), so this warn should be after calling it
          log.warn "super was not called in #start: called it forcedly", plugin: self.class
        end
      end

      def before_shutdown
        super
        unless self.before_shutdown?
          log.warn "super was not called in #before_shutdown: calling it forcedly", plugin: self.class
          @@_super_before_shutdown[self.class].bind(self).call
        end
      end

      def shutdown
        super
        unless self.shutdown?
          log.warn "super was not called in #shutdown: calling it forcedly", plugin: self.class
          @@_super_shutdown[self.class].bind(self).call
        end
      end
    end
  end
end
