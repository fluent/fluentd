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
  module Test
    class FilterTestDriver < TestDriver
      def initialize(klass, &block)
        super(klass, &block)
      end

      def filter(tag, time, record)
        @instance.filter(tag, time, record)
      end

      def filter_stream(tag, es)
        @instance.filter_stream(tag, es)
      end
    end
  end
end
