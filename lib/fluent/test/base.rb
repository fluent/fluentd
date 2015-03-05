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
    def self.setup
      Fluent.__send__(:remove_const, :Engine)
      engine = Fluent.const_set(:Engine, EngineClass.new).init

      engine.define_singleton_method(:now=) {|n|
        @now = n.to_i
      }
      engine.define_singleton_method(:now) {
        @now || super()
      }

      nil
    end

    # obsolete for v0.14
    class TestDriver
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
        begin
          # wait until thread starts
          num_waits.times { sleep 0.05 }
          return yield
        ensure
          @instance.shutdown
        end
      end
    end

    class DummyLogDevice
      attr_reader :logs

      def initialize
        @logs = []
      end

      def tty?
        false
      end

      def puts(*args)
        args.each{ |arg| write(arg + "\n") }
      end

      def write(message)
        @logs.push message
      end

      def flush
        true
      end

      def close
        true
      end
    end

    class TestLogger < Fluent::PluginLogger
      def initialize
        @logdev = DummyLogDevice.new
        super(Fluent::Log.new(@logdev))
      end

      def logs
        @logdev.logs
      end
    end
  end
end
