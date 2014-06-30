require 'fluent/plugin/socket_util'

module Fluent
  class TcpInput < SocketUtil::BaseInput
    Plugin.register_input('tcp', self)

    config_set_default :port, 5170
    config_param :delimiter, :string, :default => "\n" # syslog family add "\n" to each message and this seems only way to split messages in tcp stream

    def listen(callback)
      log.debug "listening tcp socket on #{@bind}:#{@port}"
      Coolio::TCPServer.new(@bind, @port, SocketUtil::TcpHandler, log, @delimiter, callback)
    end
  end
end
