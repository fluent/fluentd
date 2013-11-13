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
module Fluentd

  class Engine
    module ClassMethods
      # Fluentd::Logger initialized by Worker#initialize
      attr_accessor :logger
      alias_method :log, :logger

      # PluginRegistry initialized by Worker#initialize
      attr_accessor :plugins

      # SocketManager initialize by Worker#initialize
      attr_accessor :sockets

      # RootAgent initialize by Worker#configure
      attr_accessor :root_agent

      # Globally shared data initialized by Server#initialize
      attr_accessor :shared_data

      def now
        Time.now
      end

      def setup_test_environment!
        require 'fluentd/logger'
        require 'fluentd/plugin_registry'
        require 'fluentd/socket_manager'
        Engine.logger ||= Logger.new(STDERR)
        Engine.plugins ||= PluginRegistry.new
        Engine.sockets ||= SocketManager::NonManagedAPI.new
        Engine.shared_data ||= {}
        Engine.load_plugin_api!
      end

      def load_plugin_api!
        require 'fluentd/plugin'
        require 'fluentd/event_collection'
        require 'fluentd/plugin/input'
        require 'fluentd/plugin/output'
        require 'fluentd/plugin/buffer'
        require 'fluentd/plugin/buffered_output'
        require 'fluentd/plugin/object_buffered_output'
        require 'fluentd/plugin/time_sliced_output'
        require 'fluentd/plugin/filter'
      end
    end

    extend ClassMethods
  end

end
