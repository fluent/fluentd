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
        @server = ServerEngine::SocketManager::Server.open
        ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @server.path.to_s
      end

      def shutdown
        @server.close
      end

      def self.setup
        @server = ServerEngine::SocketManager::Server.open
        ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @server.path.to_s
      end

      def self.teardown
        @server.close
        # on Windows, the path is a TCP port number
        FileUtils.rm_f @server.path unless Fluent.windows?
      end
    end
  end
end
