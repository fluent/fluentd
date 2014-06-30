require 'fluent/plugin/socket_util'

module Fluent
  class UdpInput < SocketUtil::BaseInput
    Plugin.register_input('udp', self)

    config_set_default :port, 5160
    config_param :body_size_limit, :size, :default => 4096

    def listen(callback)
      log.debug "listening udp socket on #{@bind}:#{@port}"
      @usock = SocketUtil.create_udp_socket(@bind)
      @usock.bind(@bind, @port)
      SocketUtil::UdpHandler.new(@usock, log, @body_size_limit, callback)
    end
  end
end
