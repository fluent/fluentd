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

require 'fluent/plugin/output'
require 'timeout'

module Fluent
  module Test
    module Driver
      class Output < BaseOwner
        include EventFeeder

        def initialize(klass, opts: {}, &block)
          super
          raise ArgumentError, "plugin is not an instance of Fluent::Plugin::Output" unless @instance.is_a? Fluent::Plugin::Output
          @flush_buffer_at_cleanup = nil
          @wait_flush_completion = nil
          @force_flush_retry = nil
          @format_hook = nil
          @format_results = []
        end

        def run(flush: true, wait_flush_completion: true, force_flush_retry: false, **kwargs, &block)
          @flush_buffer_at_cleanup = flush
          @wait_flush_completion = wait_flush_completion
          @force_flush_retry = force_flush_retry
          super(**kwargs, &block)
        end

        def run_actual(**kwargs, &block)
          if @force_flush_retry
            @instance.retry_for_error_chunk = true
          end
          val = super(**kwargs, &block)
          if @flush_buffer_at_cleanup
            self.flush
          end
          val
        ensure
          @instance.retry_for_error_chunk = false
        end

        def formatted
          @format_results
        end

        def flush
          @instance.force_flush
          wait_flush_completion if @wait_flush_completion
        end

        def wait_flush_completion
          buffer_queue = ->(){ @instance.buffer && @instance.buffer.queue.size > 0 }
          dequeued_chunks = ->(){
            @instance.dequeued_chunks_mutex &&
            @instance.dequeued_chunks &&
            @instance.dequeued_chunks_mutex.synchronize{ @instance.dequeued_chunks.size > 0 }
          }

          Timeout.timeout(10) do
            while buffer_queue.call || dequeued_chunks.call
              sleep 0.1
            end
          end
        end

        def instance_hook_after_started
          super

          # it's decided after #start whether output plugin instances use @custom_format or not.
          if @instance.instance_eval{ @custom_format }
            @format_hook = format_hook = ->(result){ @format_results << result }
            m = Module.new do
              define_method(:format) do |tag, time, record|
                result = super(tag, time, record)
                format_hook.call(result)
                result
              end
            end
            @instance.singleton_class.prepend m
          end
        end
      end
    end
  end
end
