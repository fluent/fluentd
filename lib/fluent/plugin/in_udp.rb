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
  class UdpInput < SocketUtil::BaseInput
    Plugin.register_input('udp', self)

    config_set_default :port, 5160
    config_param :body_size_limit, :size, default: 4096

    def listen(callback)
      log.info "listening udp socket on #{@bind}:#{@port}"
      @usock = SocketUtil.create_udp_socket(@bind)
      @usock.bind(@bind, @port)
      SocketUtil::UdpHandler.new(@usock, log, @body_size_limit, callback)
    end
  end
end
