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

require 'fluent/config'
require 'fluent/config/element'
require 'fluent/log'

require 'timeout'

module Fluent
  module Test
    module Driver
      class Base
        attr_reader :instance, :logs

        DEFAULT_TIMEOUT = 300

        def initialize(klass, opts: {}, &block)
          if klass.is_a?(Class)
            if block
              # Create new class for test w/ overwritten methods
              #   klass.dup is worse because its ancestors does NOT include original class name
              klass_name = klass.name
              klass = Class.new(klass)
              klass.define_singleton_method("name") { klass_name }
              klass.module_eval(&block)
            end
            @instance = klass.new
          else
            @instance = klass
          end
          @instance.under_plugin_development = true

          @logs = []

          @run_post_conditions = []
          @run_breaking_conditions = []
          @broken = false
        end

        def configure(conf, syntax: :v1)
          raise NotImplementedError
        end

        def end_if(&block)
          raise ArgumentError, "block is not given" unless block_given?
          @run_post_conditions << block
        end

        def break_if(&block)
          raise ArgumentError, "block is not given" unless block_given?
          @run_breaking_conditions << block
        end

        def broken?
          @broken
        end

        def run(timeout: nil, start: true, shutdown: true, &block)
          instance_start if start

          if @instance.respond_to?(:thread_wait_until_start)
            @instance.thread_wait_until_start
          end
          if @instance.respond_to?(:event_loop_wait_until_start)
            @instance.event_loop_wait_until_start
          end

          begin
            run_actual(timeout: timeout, &block)
          ensure
            instance_shutdown if shutdown
          end
        end

        def instance_start
          unless @instance.started?
            @instance.start
            instance_hook_after_started
          end
          unless @instance.after_started?
            @instance.after_start
          end
        end

        def instance_hook_after_started
          # insert hooks for tests available after instance.start
        end

        def instance_shutdown
          @instance.stop            unless @instance.stopped?
          @instance.before_shutdown unless @instance.before_shutdown?
          @instance.shutdown        unless @instance.shutdown?

          if @instance.respond_to?(:event_loop_wait_until_stop)
            @instance.event_loop_wait_until_stop
          end

          @instance.after_shutdown  unless @instance.after_shutdown?
          @instance.close     unless @instance.closed?

          if @instance.respond_to?(:thread_wait_until_stop)
            @instance.thread_wait_until_stop
          end

          @instance.terminate unless @instance.terminated?
        end

        def run_actual(timeout: nil, &block)
          if @instance.respond_to?(:_threads)
            until @instance._threads.values.all?(&:alive?)
              sleep 0.01
            end
          end

          if @instance.respond_to?(:event_loop_running?)
            until @instance.event_loop_running?
              sleep 0.01
            end
          end

          timeout ||= DEFAULT_TIMEOUT
          stop_at = Time.now + timeout
          @run_breaking_conditions << ->(){ Time.now >= stop_at }

          if !block_given? && @run_post_conditions.empty? && @run_breaking_conditions.empty?
            raise ArgumentError, "no stop conditions nor block specified"
          end

          return_value = nil
          proc = if block_given?
                   ->(){ return_value = block.call; sleep(0.1) until stop? }
                 else
                   ->(){ sleep(0.1) until stop? }
                 end

          begin
            Timeout.timeout(timeout * 1.1) do |sec|
              proc.call
            end
          rescue Timeout::Error
            @broken = true
          end
          return_value
        end

        def stop?
          # Should stop running if post conditions are not registered.
          return true unless @run_post_conditions

          # Should stop running if all of the post conditions are true.
          return true if @run_post_conditions.all? {|proc| proc.call }

          # Should stop running if some of the breaking conditions is true.
          # In this case, some post conditions may be not true.
          if @run_breaking_conditions.any? {|proc| proc.call }
            @broken = true
            return true
          end

          false
        end
      end
    end
  end
end
