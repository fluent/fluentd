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

require 'fluent/test/driver/base'
require 'fluent/test/driver/test_event_router'

module Fluent
  module Test
    module Driver
      class BaseOwner < Base
        def initialize(klass, opts: {}, &block)
          super

          if opts
            @instance.system_config_override(opts)
          end
          @instance.log = TestLogger.new
          @logs = @instance.log.out.logs

          @event_streams = nil
          @error_events = nil
        end

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
            @instance.singleton_class.prepend mojule
          end

          @instance.configure(@config)
          self
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
          if block_given?
            event_streams(tag: tag) do |t, es|
              es.each do |time, record|
                yield t, time, record
              end
            end
          else
            list = []
            event_streams(tag: tag) do |t, es|
              es.each do |time, record|
                list << [t, time, record]
              end
            end
            list
          end
        end

        def event_streams(tag: nil)
          return [] if @event_streams.nil?
          selected = @event_streams.select{|e| tag.nil? ? true : e.tag == tag }
          if block_given?
            selected.each do |e|
              yield e.tag, e.es
            end
          else
            selected.map{|e| [e.tag, e.es] }
          end
        end

        def emit_count
          @event_streams.size
        end

        def record_count
          @event_streams.reduce(0) {|a, e| a + e.es.size }
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
          if expect_emits
            @run_post_conditions << ->(){ emit_count >= expect_emits }
          end
          if expect_records
            @run_post_conditions << ->(){ record_count >= expect_records }
          end

          super(timeout: timeout, start: start, shutdown: shutdown, &block)
        end
      end
    end
  end
end
