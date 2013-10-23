#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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

      # SocketManager initialize by WorkerLauncher.main
      attr_accessor :sockets

      # RootAgent initialize by Worker#configure
      attr_accessor :root_agent

      # Globally shared data initialized by Server#initialize
      attr_accessor :shared_data

      def now
        Time.now
      end

      def setup_defaults!
        logger ||= Logger.new(STDERR)
        plugins ||= PluginRegistry.new
        sockets ||= SocketManager::NonManagedAPI.new
        shared_data ||= {}
      end
    end

    extend ClassMethods
  end

end
