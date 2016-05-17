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

require 'singleton'

module Fluent
  module Compat
    # TODO: remove when old plugin API are removed
    class NullOutputChain
      include Singleton

      def next
      end
    end

    class OutputChain
      def initialize(array, tag, es, chain=NullOutputChain.instance)
        @array = array
        @tag = tag
        @es = es
        @offset = 0
        @chain = chain
      end

      def next
        if @array.length <= @offset
          return @chain.next
        end
        @offset += 1
        @array[@offset-1].emit_events(@tag, @es)
        self.next
      end
    end

    class CopyOutputChain < OutputChain
      def next
        if @array.length <= @offset
          return @chain.next
        end
        @offset += 1
        es = @array.length > @offset ? @es.dup : @es
        @array[@offset-1].emit_events(@tag, es)
        self.next
      end
    end
  end
end
