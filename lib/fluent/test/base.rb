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
require 'fluent/engine'
require 'fluent/system_config'
require 'fluent/test/log'
require 'serverengine'

module Fluent
  module Test
    class TestDriver
      include ::Test::Unit::Assertions

      def initialize(klass, &block)
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
        @instance.router = Engine.root_agent.event_router
        @instance.log = TestLogger.new
        Engine.root_agent.instance_variable_set(:@log, @instance.log)

        @config = Config.new
      end

      attr_reader :instance, :config

      def configure(str, use_v1 = false)
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

      # num_waits is for checking thread status. This will be removed after improved plugin API
      def run(num_waits = 10, &block)
        @instance.start
        @instance.after_start
        begin
          # wait until thread starts
          num_waits.times { sleep 0.05 }
          return yield
        ensure
          @instance.shutdown
        end
      end
    end
  end
end
