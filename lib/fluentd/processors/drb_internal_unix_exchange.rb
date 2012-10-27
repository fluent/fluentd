#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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
  module Processors
    require 'drb'

    class DRbInternalUNIXExchange
      def self.remote_uri(exchange_client, processor_id)
        "drbiunix:#{exchange_client.__id__.to_s(16)}:#{processor_id}"
      end

      def self.server_uri(exchange_server, exchange_clients)
        clients = exchange_clients.map {|ec| ec.__id__.to_s(16) }.join(':')
        "drbiunix:#{exchange_server.__id__.to_s(16)}/#{clients}"
      end

      def self.parse_uri(uri)
        if m = /^drbiunix:(.*?)(?:\:(.*?))?(?:\/(.*?))?(?:\?(.*))?$/.match(uri)
          cs_hexid = m[1]
          processor_id = m[2]
          client_hexids = m[3]
          option = m[4]
          if client_hexids
            server_id = cs_hexid.to_i(16)
            client_ids = client_hexids.split(':').map {|h| h.to_i(16) }
            return [server_id, nil, client_ids, option]
          else
            client_id = cs_hexid.to_i(16)
            processor_id = processor_id
            return [client_id, processor_id, nil, option]
          end
        else
          raise(DRb::DRbBadScheme, uri) unless uri =~ /^drbiunix:/
          raise(DRb::DRbBadURI, 'can\'t parse uri:' + uri)
        end
      end

      def self.open(uri, config)
        client_id, processor_id, client_ids, option = parse_uri(uri)
        #if client_id == 0
        #  # local loopback connect
        #  exchange_server = ObjectSpace._id2ref(server_id)
        #  io = exchange_server.open_io(nil)
        #else
          client_id = client_ids[processor_id] if processor_id
          exchange_client = ObjectSpace._id2ref(client_id)
          io = exchange_client.connect
        #end
        #if exchange_client.is_a?(InternalUNIXServer)
        #  # local loopback connect
        #  exchange_server = exchange_client
        #  io = exchange_server.open_io(nil)
        #else
        #  io = exchange_client.connect
        #end
        Connection.new(uri, io, config)
      end

      def self.open_server(uri, config)
        server_id, processor_id, client_ids, option = parse_uri(uri)
        if processor_id != nil
          raise NoMethodError, "Exchanging non-marshallable objects is not supported with multiprocess"
        end
        exchange_server = ObjectSpace._id2ref(server_id)
        Server.new(uri, exchange_server, config)
      end

      def self.uri_option(uri, config)
        cs_id, processor_id, client_ids, option = parse_uri(uri)
        if processor_id
          client_id = cs_id
          processor_id = cs_id
          return "drbiunix:#{client_id.to_s(16)}:#{processor_id}", option
        else
          server_id = cs_id
          clients = client_ids.map {|cid| cid.to_s(16) }.join(':')
          return "drbiunix:#{server_id.to_s(16)}:#{clients}", option
        end
      end

      class Server
        def initialize(uri, exchange_server, config)
          @uri = uri
          @exchange_server = exchange_server
          @config = config
        end

        attr_reader :uri

        def accept
          io = @exchange_server.accept
          Connection.new(nil, io, @config)
        end

        def close
          # do nothing
        end

        def alive?
          true
        end
      end

      class Connection
        def initialize(uri, io, config)
          @uri = uri
          @io = io
          @config = config
          @msg = DRb::DRbMessage.new(config)
        end

        attr_reader :uri

        def peeraddr
          # TODO
          @io.peeraddr
        end

        def send_request(ref, msg_id, arg, b)
          @msg.send_request(@io, ref, msg_id, arg, b)
        end

        def recv_request
          @msg.recv_request(@io)
        end

        def send_reply(succ, result)
          @msg.send_reply(@io, succ, result)
        end

        def recv_reply
          @msg.recv_reply(@io)
        end

        def close
          @io.close unless @io.closed?
        end

        def alive?
          if IO.select([@io], nil, nil, 0)
            close
            return false
          end
          true
        end
      end
    end

    DRb::DRbProtocol.add_protocol(DRbInternalUNIXExchange)
  end
end

