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

require 'fluent/test/driver/base_owner'
require 'fluent/plugin/input'

module Fluent
  module Test
    module Driver
      class Input < BaseOwner
        def initialize(klass, opts: {}, &block)
          super
          raise ArgumentError, "plugin is not an instance of Fluent::Plugin::Input" unless @instance.is_a? Fluent::Plugin::Input
        end
      end
    end
  end
end
