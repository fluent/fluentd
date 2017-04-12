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

require 'fluent/plugin'
require 'fluent/plugin/input'
require 'fluent/process'
require 'fluent/compat/call_super_mixin'

module Fluent
  module Compat
    class Input < Fluent::Plugin::Input
      # TODO: warn when deprecated

      def initialize
        super
        unless self.class.ancestors.include?(Fluent::Compat::CallSuperMixin)
          self.class.prepend Fluent::Compat::CallSuperMixin
        end
      end

      # These definitions are to get instance methods of superclass of 3rd party plugins
      # to make it sure to call super
      def start
        super
      end

      def before_shutdown
        super
      end

      def shutdown
        super
      end
    end
  end
end
