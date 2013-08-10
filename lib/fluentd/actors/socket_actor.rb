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
  module Actors

    module SocketActor
      def create_tcp_thread_server(bind, port, &block)
        listen_socket = listen_tcp(bind, port)
        listen_io(listen_socket) do |io|
          background(io, &callback)
        end
      end

      def create_unix_thread_server(path, &block)
        listen_socket = Fluentd.socket_manager.listen_unix(path)
        listen_io(listen_socket) do |io|
          background(io, &callback)
        end
      end

      def listen_io(listen_socket, &callback)
        @loop.attach CallbackListener.new(listen_socket, &callback)
      end

      def listen_tcp(bind, port, &block)
        socket = Fluentd.socket_manager.listen_tcp(bind, port)
        listen_io(socket, &block)
      end

      def listen_unix(path, &block)
        socket = Fluentd.socket_manager.listen_unix(path)
        listen_io(socket, &block)
      end

      def listen_udp(bind, port, &block)
        socket = Fluentd.socket_manager.listen_udp(bind, port)
        watch_io(socket, &block)
      end

      class CallbackListener < Coolio::Listener
        def initialize(listen_socket, &callback)
          @callback = callback
          super(listen_socket)
        end

        def on_connection(socket)
          @callback.call(socket)
        rescue
          Fluentd.log.error $!.to_s
          Fluentd.log.error_backtrace
        ensure
          sock.close
        end
      end
    end

  end
end
