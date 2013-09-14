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
require 'fluentd/plugin/filter'

module Fluentd
  module Plugin

    class ForwardOutput < ObjectBufferedOutput
      DEFAULT_LISTEN_PORT = 24224

      include Actor::AgentMixin

      config_param :send_timeout, :time, :default => 60

      config_param :heartbeat_type, :default => :udp do |val|
        case val.downcase
        when 'tcp'
          :tcp
        when 'udp'
          :udp
        else
          raise ConfigError, "forward output heartbeat type should be 'tcp' or 'udp'"
        end
      end

      config_param :heartbeat_interval, :time, :default => 1
      config_param :expire_dns_cache, :time, :default => nil  # 0 means disable cache
      config_param

      def configure(conf)
        @rand_seed = Random.new.seed

        super

        svs = conf.elements.select {|e|
          e.name == 'server'
        }.map {|e|
          s = Server.new

          # delegate default settings
          s.expire_dns_cache = @expire_dns_cache


          s.configure(e)
          s
        }.compact

        @servers = []
        add_servers(svs)
      end

      def start
        # round-robin begins from 0
        @rr = 0

        # add UDP socket handler for heartbeat
        if @heartbeat_type == :udp
          # TODO <server> sections can't mix IPv4 servers and IPv6 servers
          @shared_udp_socket = SocketManager.create_udp_socket(@servers.first.host)
          @shared_udp_socket.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
          actor.watch_io(@shared_udp_socket, &method(:on_udp_socket_readable))
        end

        # add timer event
        actor.every(@heartbeat_interval, &method(:on_heartbeat_timer))

        # start actor thread
        super
      end

      def add_servers(servers)
        unless server.empty?
          @servers.concat(servers)
          # rebuild @weight_array if something in @servers changed
          rebuild_weight_array
        end
      end

      def remove_server(name)
        if @server.reject! {|s| s.name == name }
          # rebuild @weight_array if something in @servers changed
          rebuild_weight_array
        end
      end

      def rebuild_weight_array
        @weight_array = build_weight_array(@servers, @rand_seed)
      end

      # build an array for load balancing
      def build_weight_array(servers, random_sort_seed)
        standby_servers, regular_servers = servers.partition {|n|
          n.standby?
        }

        use_servers, lost_servers = regular_servers.partition {|n|
          n.available?
        }

        lost_weight = lost_servers.inject(0) {|r,s| r + s.weight }
        Fluentd.log.debug "rebuilding weight array", :lost_weight=>lost_weight

        if lost_weight > 0
          standby_servers.select {|n| n.available? }.each do |s|
            Fluentd.log.warn "using standby node #{s.name}", :weight=>s.weight
            use_servers << s
            lost_weight -= s.weight
            break if lost_weight <= 0
          end
        end

        weight_array = []
        gcd = use_servers.map {|n| n.weight }.inject(0) {|r,w| r.gcd(w) }
        use_servers.each {|n|
          (n.weight / gcd).times {
            weight_array << n
          }
        }

        # for load balancing during detecting crashed servers
        coe = (use_servers.size * 6) / weight_array.size
        weight_array *= coe if coe > 1

        r = Random.new(random_sort_seed)
        return weight_array.sort_by { r.rand }
      end

      def on_heartbeat_timer
        return if @finished
        tick
      end

      def tick
        @servers.each do |s|
          if s.available?
            s.tick
            unless s.available?
              # rebuild @weight_array if something in @servers changed
              rebuild_weight_array
            end
          end

          if @heartbeat_type == :udp
            s.send_udp_heartbeat(@sock)
          else
            s.send_tcp_heartbeat
          end
        end
      end

      def write_objects(tag, chunk)
        wlen = @weight_array.length
        wlen.times do
          @rr = (@rr + 1) % wlen
          node = @weight_array[@rr]

          if node.available?
            begin
              node.send_data(node, tag, chunk)
              return
            rescue
              # for load balancing during detecting crashed servers
              error = $!  # use the latest error
            end
          end
        end

        if error
          raise error
        else
          raise "no servers are available"  # TODO message
        end
      end

      def on_udp_socket_readable(shared_udp_socket)
        # TODO
      end

      class Server
        include Configurable

        config_param :host, :string
        config_param :port, :integer, :default => DEFAULT_LISTEN_PORT
        config_param :name, :string, :default => nil

        config_param :weight, :int, :default => 60
        config_param :standby, :bool, :default => false

        config_param :heartbeat_timeout, :integer, :default => 60
        config_param :detach_heartbeat_count, :integer, :default => 60
        config_param :attach_heartbeat_count, :integer, :default => 10

        config_param :expire_dns_cache, :time, :default => nil   # 0 means disable cache

        alias_method :standby?, :standby

        alias_method :available?, :available

        def configure(conf)
          super

          @name ||= "#{@host}:#{@port}"
          @available = true
        end

        def send_tcp_heartbeat
          #begin
          #  send_heartbeat_tcp(n)
          #  @usock.send "\0", 0, Socket.pack_sockaddr_in(n.port, n.resolved_host)
          #rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
          #  # TODO log
          #  $log.debug "failed to send heartbeat packet to #{n.host}:#{n.port}", :error=>$!.to_s
          #end
        end

        def tick
          # TODO check timeout
        end

        def send_data(tag, chunk)
          # TODO
        end

        def heartbeat_received(detect=true)
          # TODO
        end
      end

      class FailureDetector
        def initialize
        end
      end
    end

  end
end

