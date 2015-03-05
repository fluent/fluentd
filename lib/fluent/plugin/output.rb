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

require 'fluent/plugin/base'

module Fluent
  module Plugin
    class Output < Base
      def initialize
        super
      end

      def configure(conf)
        super
      end

      def start
        super
      end

      def shutdown
        super
      end

      def emit(tag, es, chain)
        #### TODO:
      end

      def secondary_init(primary)
        if primary.class != self.class
          $log.warn "type of secondary output should be same as primary output", primary: primary.class.to_s, secondary: self.class.to_s
        end
      end

      def inspect; "#<%s:%014x>" % [self.class.name, '0x%014x' % (__id__ << 1)] end
    end
  end
end
