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
require 'fluent/clock'

require 'serverengine/socket_manager'
require 'fileutils'
require 'timeout'

module Fluent
  module Test
    module Driver
      class TestTimedOut < RuntimeError; end

      class Base
        attr_reader :instance, :logs

        DEFAULT_TIMEOUT = 300

        def initialize(klass, opts: {}, &block)
          if klass.is_a?(Class)
            @instance = klass.new
            if block
              @instance.singleton_class.module_eval(&block)
              @instance.send(:initialize)
            end
          else
            @instance = klass
          end
          @instance.under_plugin_development = true

          @socket_manager_server = nil

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

          timeout ||= DEFAULT_TIMEOUT
          stop_at = Fluent::Clock.now + timeout
          @run_breaking_conditions << ->(){ Fluent::Clock.now >= stop_at }

          if !block_given? && @run_post_conditions.empty? && @run_breaking_conditions.empty?
            raise ArgumentError, "no stop conditions nor block specified"
          end

          sleep_with_watching_threads = ->(){
            if @instance.respond_to?(:_threads)
              @instance._threads.values.each{|t| t.join(0) }
            end
            sleep 0.1
          }

          begin
            retval = run_actual(timeout: timeout, &block)
            sleep_with_watching_threads.call until stop?
            retval
          ensure
            instance_shutdown if shutdown
          end
        end

        def instance_start
          if @instance.respond_to?(:server_wait_until_start)
            @socket_manager_path = ServerEngine::SocketManager::Server.generate_path
            if @socket_manager_path.is_a?(String) && File.exist?(@socket_manager_path)
              FileUtils.rm_f @socket_manager_path
            end
            @socket_manager_server = ServerEngine::SocketManager::Server.open(@socket_manager_path)
            ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @socket_manager_path.to_s
          end

          unless @instance.started?
            @instance.start
          end
          unless @instance.after_started?
            @instance.after_start
          end

          if @instance.respond_to?(:thread_wait_until_start)
            @instance.thread_wait_until_start
          end

          if @instance.respond_to?(:event_loop_wait_until_start)
            @instance.event_loop_wait_until_start
          end

          instance_hook_after_started
        end

        def instance_hook_after_started
          # insert hooks for tests available after instance.start
        end

        def instance_hook_before_stopped
          # same with above
        end

        def instance_shutdown
          instance_hook_before_stopped

          show_errors_if_exists = ->(label, block){
            begin
              block.call
            rescue => e
              puts "unexpected error while #{label}, #{e.class}:#{e.message}"
              e.backtrace.each do |bt|
                puts "\t#{bt}"
              end
            end
          }

          show_errors_if_exists.call(:stop,            ->(){ @instance.stop unless @instance.stopped? })
          show_errors_if_exists.call(:before_shutdown, ->(){ @instance.before_shutdown unless @instance.before_shutdown? })
          show_errors_if_exists.call(:shutdown,        ->(){ @instance.shutdown unless @instance.shutdown? })
          show_errors_if_exists.call(:after_shutdown,  ->(){ @instance.after_shutdown unless @instance.after_shutdown? })

          if @instance.respond_to?(:server_wait_until_stop)
            @instance.server_wait_until_stop
          end

          if @instance.respond_to?(:event_loop_wait_until_stop)
            @instance.event_loop_wait_until_stop
          end

          show_errors_if_exists.call(:close, ->(){ @instance.close unless @instance.closed? })

          if @instance.respond_to?(:thread_wait_until_stop)
            @instance.thread_wait_until_stop
          end

          show_errors_if_exists.call(:terminate, ->(){ @instance.terminate unless @instance.terminated? })

          if @socket_manager_server
            @socket_manager_server.close
            if @socket_manager_server.is_a?(String) && File.exist?(@socket_manager_path)
              FileUtils.rm_f @socket_manager_path
            end
          end
        end

        def run_actual(timeout: DEFAULT_TIMEOUT, &block)
          if @instance.respond_to?(:_threads)
            sleep 0.1 until @instance._threads.values.all?(&:alive?)
          end

          if @instance.respond_to?(:event_loop_running?)
            sleep 0.1 until @instance.event_loop_running?
          end

          if @instance.respond_to?(:_child_process_processes)
            sleep 0.1 until @instance._child_process_processes.values.all?{|pinfo| pinfo.alive }
          end

          return_value = nil
          begin
            Timeout.timeout(timeout * 2) do |sec|
              return_value = block.call if block_given?
            end
          rescue Timeout::Error
            raise TestTimedOut, "Test case timed out with hard limit."
          end
          return_value
        end

        def stop?
          # Should stop running if post conditions are not registered.
          return true unless @run_post_conditions || @run_post_conditions.empty?

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
