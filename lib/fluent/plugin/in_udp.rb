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

    desc "Deprecated parameter. Use message_length_limit instead"
    config_param :body_size_limit, :size, default: nil, deprecated: "use message_length_limit instead."
    desc "The max bytes of message"
    config_param :message_length_limit, :size, default: 4096
    desc "Remove newline from the end of incoming payload"
    config_param :remove_newline, :bool, default: true
    desc "The max size of socket receive buffer. SO_RCVBUF"
    config_param :receive_buffer_size, :size, default: nil

    def configure(conf)
      super

      @message_length_limit = @body_size_limit if @body_size_limit
    end

    def listen(callback)
      log.info "listening udp socket on #{@bind}:#{@port}"
      @usock = SocketUtil.create_udp_socket(@bind)
      @usock.bind(@bind, @port)
      SocketUtil::UdpHandler.new(@usock, log, @message_length_limit, callback, !!@source_hostname_key, @remove_newline, @receive_buffer_size)
    end
  end
end
