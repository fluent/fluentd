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

require 'test/unit'

module Fluent
  module Test
    module Driver
      class Base
        include ::Test::Unit::Assertions

        def initialize(klass, &block)
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
          @instance.router = Engine.root_agent.event_router
          @instance.log = TestLogger.new

          @config = Config.new
        end

        attr_reader :instance, :config

        def configure(str, use_v1=true) # v1 syntax default
          if str.is_a?(Fluent::Config::Element)
            @config = str
          else
            @config = Config.parse(str, "(test)", "(test_dir)", use_v1)
          end
          if label_name = @config['@label']
            Engine.root_agent.add_label(label_name)
          end
          @instance.configure(@config)
          self
        end

        def run(&block)
          @instance.start
          if @instance.respond_to?(:event_loop_running?)
            until @instance.event_loop_running?
              sleep 0.01
            end
          end
          if @instance.respond_to?(:_threads)
            until @instance._threads.values.all?(&:alive?)
              sleep 0.01
            end
          end
          begin
            yield
          ensure
            @instance.stop
            @instance.shutdown
            @instance.close
            @instance.terminate
          end
        end

        def stop?
          # Should stop running if post conditions are not registered.
          return true unless @run_post_conditions

          # Should stop running if all of the post conditions are true.
          return true if @run_post_conditions.all? {|proc| proc.call }

          # Should stop running if any of the breaking conditions is true.
          # In this case, some post conditions may be not true.
          return true if @run_breaking_conditions && @run_breaking_conditions.any? {|proc| proc.call }

          false
        end

        def end_if(&block)
          if block
            @run_post_conditions ||= []
            @run_post_conditions << block
          end
        end

        def break_if(&block)
          if block
            @run_breaking_conditions ||= []
            @run_breaking_conditions << block
          end
        end
      end
    end
  end
end
