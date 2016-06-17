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

require 'fluent/config/element'
require 'fluent/log'
require 'fluent/test/driver/test_event_router'

require 'timeout'

module Fluent
  module Test
    module Driver
      class Base
        def initialize(klass, opts: {}, &block)
          if klass.is_a?(Class)
            if block
              # Create new class for test w/ overwritten methods
              #   klass.dup is worse because its ancestors does NOT include original class name
              klass = Class.new(klass)
              klass.module_eval(&block)
            end
            @instance = klass.new
          else
            @instance = klass
          end
          if opts
            @instance.system_config_override(opts)
          end
          @instance.log = TestLogger.new
          @logs = @instance.log.out.logs

          @run_post_conditions = []
          @run_breaking_conditions = []

          @broken = false

          @event_streams = nil
          @error_events = nil
        end

        attr_reader :instance, :logs

        def configure(conf, syntax: :v1)
          if conf.is_a?(Fluent::Config::Element)
            @config = conf
          else
            @config = Config.parse(conf, "(test)", "(test_dir)", syntax: syntax)
          end

          if @instance.respond_to?(:router=)
            @event_streams = []
            @error_events = []

            driver = self
            mojule = Module.new do
              define_method(:event_emitter_router) do |label_name|
                TestEventRouter.new(driver)
              end
            end
            @instance.singleton_class.module_eval do
              prepend mojule
            end
          end

          @instance.configure(@config)
          self
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

        Emit = Struct.new(:tag, :es)
        ErrorEvent = Struct.new(:tag, :time, :record, :error)

        # via TestEventRouter
        def emit_event_stream(tag, es)
          @event_streams << Emit.new(tag, es)
        end

        def emit_error_event(tag, time, record, error)
          @error_events << ErrorEvent.new(tag, time, record, error)
        end

        def events(tag: nil)
          return [] if @event_streams.nil?
          selected = @event_streams.select{|e| tag.nil? ? true : e.tag == tag }
          if block_given?
            selected.each do |e|
              e.es.each do |time, record|
                yield e.tag, time, record
              end
            end
          else
            list = []
            selected.each do |e|
              e.es.each do |time, record|
                list << [e.tag, time, record]
              end
            end
            list
          end
        end

        def error_events(tag: nil)
          selected = @error_events.select{|e| tag.nil? ? true : e.tag == tag }
          if block_given?
            selected.each do |e|
              yield e.tag, e.time, e.record, e.error
            end
          else
            selected.map{|e| [e.tag, e.time, e.record, e.error] }
          end
        end

        def run(expect_emits: nil, expect_records: nil, timeout: nil, start: true, shutdown: true, &block)
          instance_start if start

          if @instance.respond_to?(:thread_wait_until_start)
            @instance.thread_wait_until_start
          end
          if @instance.respond_to?(:event_loop_wait_until_start)
            @instance.event_loop_wait_until_start
          end

          begin
            run_actual(expect_emits: expect_emits, expect_records: expect_records, timeout: timeout, &block)
          ensure
            instance_shutdown if shutdown
          end
        end

        def instance_start
          unless @instance.started?
            @instance.start
            instance_hook_after_started
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

        def run_actual(expect_emits: nil, expect_records: nil, timeout: nil, &block)
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

          if expect_emits
            @run_post_conditions << ->(){ @emit_streams.size >= expect_emits }
          end
          if expect_records
            @run_post_conditions << ->(){ @emit_streams.reduce(0){|a, e| a + e.es.size } >= expected_records }
          end
          if timeout
            stop_at = Time.now + timeout
            @run_breaking_conditions << ->(){ Time.now >= stop_at }
          end

          if !block_given? && @run_post_conditions.empty? && @run_breaking_conditions.empty?
            raise ArgumentError, "no stop conditions nor block specified"
          end

          if !block_given?
            block = ->(){ sleep(0.1) until stop? }
          end

          if timeout
            begin
              Timeout.timeout(timeout * 1.1) do |sec|
                block.call
              end
            rescue Timeout::Error
              @broken = true
            end
          else
            block.call
          end
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
