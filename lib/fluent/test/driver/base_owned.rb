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
require 'fluent/test/driver/owner'

module Fluent
  module Test
    module Driver
      class BaseOwned
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
          owner = Fluent::Test::Driver::Owner.new
          if opts
            owner.system_config_override(opts)
          end
          owner.log = TestLogger.new

          if @instance.respond_to?(:owner=)
            @instance.owner = owner
            if opts
              @instance.system_config_override(opts)
            end
          end

          @logs = owner.log.out.logs
          @section_name = ''
        end

        attr_reader :instance, :logs

        def configure(conf, syntax: :v1)
          if conf.is_a?(Fluent::Config::Element)
            @config = conf
          elsif conf.is_a?(Hash)
            @config = Fluent::Config::Element.new(@section_name, "", Hash[conf.map{|k,v| [k.to_s, v]}], [])
          else
            @config = Fluent::Config.parse(conf, @section_name, "", syntax: syntax)
          end
          @instance.configure(@config)
          self
        end

        def run(start: true, shutdown: true, &block)
          instance_start if start

          begin
            yield
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
          @instance.terminate unless @instance.terminated?
        end
      end
    end
  end
end
