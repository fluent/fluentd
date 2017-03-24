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

require 'cool.io'

require 'fluent/plugin/socket_util'

module Fluent
  class TcpInput < SocketUtil::BaseInput
    Plugin.register_input('tcp', self)

    config_set_default :port, 5170
    desc 'The payload is read up to this character.'
    config_param :delimiter, :string, default: "\n" # syslog family add "\n" to each message and this seems only way to split messages in tcp stream

    def listen(callback)
      log.info "listening tcp socket on #{@bind}:#{@port}"
      Coolio::TCPServer.new(@bind, @port, SocketUtil::TcpHandler, log, @delimiter, callback, !!@source_hostname_key)
    end
  end
end
