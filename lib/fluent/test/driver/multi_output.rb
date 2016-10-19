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
require 'fluent/test/driver/event_feeder'

require 'fluent/plugin/multi_output'

module Fluent
  module Test
    module Driver
      class MultiOutput < BaseOwner
        include EventFeeder

        def initialize(klass, opts: {}, &block)
          super
          raise ArgumentError, "plugin is not an instance of Fluent::Plugin::MultiOutput" unless @instance.is_a? Fluent::Plugin::MultiOutput
          @flush_buffer_at_cleanup = nil
        end

        def run(flush: true, **kwargs, &block)
          @flush_buffer_at_cleanup = flush
          super(**kwargs, &block)
        end

        def run_actual(**kwargs, &block)
          val = super(**kwargs, &block)
          if @flush_buffer_at_cleanup
            @instance.outputs.each{|o| o.force_flush }
          end
          val
        end

        def flush
          @instance.outputs.each{|o| o.force_flush }
        end
      end
    end
  end
end
