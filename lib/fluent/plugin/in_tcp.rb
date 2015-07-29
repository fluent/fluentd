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

require 'fluent/plugin/socket_util'

module Fluent
  class TcpInput < SocketUtil::BaseInput
    Plugin.register_input('tcp', self)

    config_set_default :port, 5170
    config_param :delimiter, :string, :default => "\n"

    def test(buffer, offset)
      i = buffer.index(@delimiter, offset)
      i ? [i, i + @delimiter.length] :  [nil, nil]
    end

    def listen(callback)
      log.debug "listening tcp socket on #{@bind}:#{@port}"
      Coolio::TCPServer.new(@bind, @port, SocketUtil::TcpHandler, log, method(:test), callback)
    end
  end
end
