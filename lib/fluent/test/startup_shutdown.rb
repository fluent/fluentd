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

require 'serverengine'
require 'fileutils'

module Fluent
  module Test
    module StartupShutdown
      def startup
        socket_manager_path = ServerEngine::SocketManager::Server.generate_path
        @server = ServerEngine::SocketManager::Server.open(socket_manager_path)
        ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = socket_manager_path.to_s
      end

      def shutdown
        @server.close
      end

      def self.setup
        @socket_manager_path = ServerEngine::SocketManager::Server.generate_path
        @server = ServerEngine::SocketManager::Server.open(@socket_manager_path)
        ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @socket_manager_path.to_s
      end

      def self.teardown
        @server.close
        # on Windows, socket_manager_path is a TCP port number
        FileUtils.rm_f @socket_manager_path unless Fluent.windows?
      end
    end
  end
end
