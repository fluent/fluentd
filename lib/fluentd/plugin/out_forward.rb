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
require 'fluentd/plugin/object_buffered_output'
require 'fluentd/dns_resolver'

module Fluentd
  module Plugin
    class ForwardOutput < ObjectBufferedOutput
      #TODO: logs on attach/detach nodes
      #TODO: check working of buffers and its re-flushing during/after all nodes detachment
      #TODO: tests for tcp heartbeating
      #TODO: tests for tcp keepalive connections
      #TODO: fix dns resolve expiration
      #TODO: error classes
      #TODO: rescue each Errno::EXXX on #send_data and #send_tcp_heartbeat
      #TODO: proper error class for heartbeat ack packets from unknown nodes

      #TODO: performance tests

      NODE_STATUS_WATCH_INTERVAL = 1

      KEEPALIVE_EXPIRED_WATCH_INTERVAL = 1
      KEEPALIVE_MAINTAIN_CONNECTION_INTERVAL = 5

      Plugin.register_output('forward', self)

      config_param :send_timeout, :time, :default => 60
      config_param :ipv6, :bool, :default => false
      config_param :expire_dns_cache, :time, :default => nil # 0 means disalbe cache, nil means cache infinitely

      config_param :keepalive, :bool, :default => false
      config_param :keepalive_time, :time, :default => nil # infinite

      config_param :heartbeat_type, :default => :udp do |val|
        case val.downcase
        when 'none' then :none
        when 'tcp' then :tcp
        when 'udp' then :udp
        else
          raise ConfigError, "forward output heartbeat type should be 'tcp', 'udp' or 'none'"
        end
      end
      config_param :heartbeat_interval, :time, :default => 1

      config_param :failure_time, :time, :default => 3
      config_param :recover_time, :time, :default => 5

      attr_reader :nodes, :usock

      def initialize
        super

        require 'socket'
        require 'ipaddr'
        require 'resolv'
      end

      def configure(conf)
        super

        if @heartbeat_type == :none && !@keepalive
          raise ConfigError, "'heartbeat_type none' is available only with 'keepalive yes'"
        end

        @nodes = []
        conf.elements.select{|e| e.name == 'server'}.each do |e|
          n = Node.new(self)
          n.configure(e)
          @nodes.push(n)
        end
        @nodes.each do |node|
          node.address # check not to raise ResolveError
        end
      end

      def start
        @running = true

        @rand_seed = Random.new.seed
        @weight_array = rebuild_balancing(@nodes)
        @rr = 0

        @nodes.each do |node|
          node.start(actor)
        end

        actor.every(NODE_STATUS_WATCH_INTERVAL) do
          next unless @running

          status_changed = @nodes.inject(false){ |r,a| a.check_alive! || r }
          if status_changed
            @weight_array = rebuild_balancing(@nodes)
          end
        end

        super # this 'super' must be end of method for actor.run
      end

      def stop
        @running = false
        @nodes.each do |node|
          node.stop
        end

        super
      end

      def shutdown
        @nodes.each do |node|
          node.shutdown
        end

        super
      end

      def write_objects(tag, chunk)
        return if chunk.empty?

        error = nil

        wlen = @weight_array.length
        wlen.times do
          @rr = (@rr + 1) % wlen
          node = @weight_array[@rr]

          if node.available?
            begin
              node.send_data(tag, chunk)
              return
            rescue => e
              # for load balancing during detecting crashed servers
              error = e
            end
          end
        end

        if error
          raise error
        else
          raise "no nodes are available" #TODO: error class
        end
      end

      def rebuild_balancing(nodes)
        standby_nodes, using_nodes = nodes.partition {|n|
          n.standby?
        }

        lost_weight = 0
        using_nodes.each do |n|
          lost_weight += n.weight unless n.available?
        end

        if lost_weight > 0
          standby_nodes.each do |n|
            next unless n.available?

            using_nodes << n
            lost_weight -= n.weight
            break if lost_weight <= 0
          end
        end

        weight_array = []
        # Integer#gcd => gcd: greatest common divisor
        gcd = using_nodes.map {|n| n.weight }.inject(0) {|r,w| r.gcd(w) }
        using_nodes.each do |n|
          (n.weight / gcd).times do
            weight_array << n
          end
        end

        # for load balancing during detecting crashed servers
        coe = (using_nodes.size * 6) / weight_array.size
        weight_array *= coe if coe > 1

        r = Random.new(@rand_seed)
        weight_array.sort_by{ r.rand }
      end

      class Node
        include Configurable

        config_param :host, :string
        config_param :port, :integer, :default => 24224

        config_param :ipv6, :bool, :default => nil
        config_param :expire_dns_cache, :time, :default => nil

        config_param :weight, :integer, :default => 60
        config_param :standby, :bool, :default => false

        config_param :keepalive, :bool, :default => nil
        config_param :keepalive_time, :time, :default => nil

        config_param :send_timeout, :time, :default => nil
        config_param :heartbeat_interval, :time, :default => nil

        config_param :failure_time, :time, :default => nil
        config_param :recover_time, :time, :default => nil

        attr_reader :nodes, :usock
        attr_reader :log

        def initialize(parent)
          super()
          @parent = parent
          @log = parent.log
        end

        def configure_parent_default(*params)
          params.each do |param|
            if self.send(param).nil?
              self.send("#{param}=".to_sym, @parent.send(param))
            end
          end
        end

        def configure(conf)
          super

          @name = "#{@host}:#{@port}"
          @proto = @ipv6 ? :ipv6 : :ipv4

          configure_parent_default :ipv6, :expire_dns_cache, :keepalive, :keepalive_time, \
                                   :send_timeout, :heartbeat_interval, :failure_time, :recover_time

          @heartbeat_type = @parent.heartbeat_type

          @ipaddr_refresh_interval = @expire_dns_cache
          @ipaddr = nil # unresolved
          @ipaddr_expires = Time.now - 1

          @state = if @heartbeat_type != :none
                     StateMachine.new(@failure_time, @recover_time)
                   else
                     nil
                   end
        end

        def standby? ; @standby ; end
        def keepalive? ; @keepalive ; end
        def available? ; @state ? @state.available? : !!@connection ; end
        def check_alive! ; @state.check! ; end
        def update_alive! ; @state.update! ; end

        def start(actor)
          @running = true
          @stopped = false

          @state.start

          @connection = nil
          @connection_expired_at = nil
          @expired_connections = []
          @conn_mutex = Mutex.new

          unless @heartbeat_type == :none
            actor.every(@heartbeat_interval) do
              next unless @running
              log.trace "sending heartbeat..."
              send_heartbeat
            end
          end

          if @heartbeat_type == :udp
            #TODO: expiration with name expired
            @usock = UDPSocket.new( @ipv6 ? Socket::AF_INET6 : Socket::AF_INET ) # Engine.sockets.listen_udp(self.address, @port)
            @usock.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
            actor.watch_io(@usock, &method(:on_udp_heartbeat_readable))
          end

          if @keepalive
            # expired connection collector
            actor.every(KEEPALIVE_EXPIRED_WATCH_INTERVAL) do
              next if @stopped
              garbages = @conn_mutex.synchronize do
                expireds,@expired_connections = @expired_connections,[]
                expireds
              end
              begin
                garbages.each{|e| e.close }
              rescue
                # all errors are ignored for garbage sockets
              end
            end

            # keepalive connection maintainer
            actor.every(KEEPALIVE_MAINTAIN_CONNECTION_INTERVAL) do
              next unless @running
              connect do |c|
                # do nothing and release connection immediately
              end
            end
          end
        end

        def stop
          @running = false
        end

        def shutdown
          @stopped = true
          begin
            @connection.close if @connection
          rescue => e
            # ignore all errors with only logging
            log.warn "error on connection closing in shutdown", :error_class => e.class, :error => e
          end
          @expired_connections.each do |c|
            begin
              c.lose
            rescue => e
              # ignore all errors with only logging
              log.warn "error on expired connection closing in shutdown", :error_class => e.class, :error => e
            end
          end
        end

        def address
          # @ipaddre_expires:  0 means to disable cache, nil means to cache infinitely
          return @ipaddr if @ipaddr && (@ipaddr_expires.nil? || Time.now < @ipaddr_expires)

          @ipaddr = DNSResolver.new(@proto).resolve(@host)
          if @ipaddr_refresh_interval && @ipaddr_refresh_interval > 0
            @ipaddr_expires = Time.now + @ipaddr_refresh_interval
          end
          @ipaddr
        end

        def open_tcp_socket
          sock = TCPSocket.new(self.address, @port)
          opt = [1, @send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
          sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
          opt = [@send_timeout.to_i, 0].pack('L!L!')  # struct timeval
          sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)
          sock
        end

        def connect(opts={}) # with block
          raise ArgumentError, "connect needs block" unless block_given?

          ### In stop status (!@running), keepalive is disabled forcely

          if !@keepalive || opts[:new_sock] || !@running
            val = nil
            begin
              sock = open_tcp_socket
              val = yield sock
            ensure
              sock.close
            end
            return val
          end

          # keepalive
          @conn_mutex.synchronize do
            if @connection && Time.now > @connection_expired_at
              @expired_connections.push(@connection)
              @connection = nil
            end
          end
          begin
            conn = @connection
            unless conn
              @connection = conn = open_tcp_socket # this may block
              @connection_expired_at = Time.now + @keepalive_time
            end
            retval = yield conn
            if conn != @connection # race condition on '@connection' and 'c'
              @expired_connections.push(conn)
            end
            retval
          rescue => e
            # discard connection for any arrors
            @conn_mutex.synchronize do
              if conn == @connection
                @connection = nil
              end
            end
            @expired_connections.push(conn)
            raise e
          end
        end

        # MessagePack FixArray length = 2
        FORWARD_HEADER = [0x92].pack('C')

        def send_data(tag, chunk)
          begin
            connect do |conn|
              # beginArray(2)
              conn.write FORWARD_HEADER
              # writeRaw(tag)
              conn.write tag.to_msgpack  # tag

              # beginRaw(size)
              sz = chunk.size

              #if sz < 32
              #  # FixRaw
              #  sock.write [0xa0 | sz].pack('C')
              #elsif sz < 65536
              #  # raw 16
              #  sock.write [0xda, sz].pack('Cn')
              #else
              # raw 32
              conn.write [0xdb, sz].pack('CN')
              #end

              # writeRawBody(packed_es)
              chunk.write_to(conn)
            end
            @state.update!
          rescue SystemCallError => e #TODO: or each Errno::EXXX
            log.error "failed to send data", :host => @host, :port => @port, :error_class => e.class, :error => e
            raise e
          end
        end

        def send_heartbeat
          if @heartbeat_type == :tcp
            send_tcp_heartbeat
          elsif @heartbeat_type == :udp
            send_udp_heartbeat
          else
            # :none
          end
        end

        # FORWARD_TCP_HEARTBEAT_DATA = FORWARD_HEADER + ''.to_msgpack + [].to_msgpack
        def send_tcp_heartbeat
          log.trace "sending tcp heartbeat"
          begin
            connect(:new_sock => true) do |conn|
              opt = [1, @send_timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
              sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
              opt = [@send_timeout.to_i, 0].pack('L!L!')  # struct timeval
              sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDTIMEO, opt)

              ### don't send any data to not cause a compatibility problem
              #sock.write FORWARD_TCP_HEARTBEAT_DATA
            end
            # success to connect => receive ack packet => alive
            @state.update!
          rescue SystemCallError => e # or each Errno::EXXX
            log.debug "failed to send tcp heartbeat", :error_class => e.class, :error => e
          end
        end

        def send_udp_heartbeat
          log.trace "sending udp heartbeat"
          begin
            @usock.send "\0", 0, self.address, @port
          rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
            log.debug "failed to send udp heartbeat", :error_class => e.class, :error => e
          end
        end

        def on_udp_heartbeat_readable(io_watcher) # we want @usock
          log.trace "reading udp heartbeat"

          begin
            msg, addr = @usock.recvfrom(1024)
          rescue Errno::EAGAIN, Errno::EWOULDBLOCK, Errno::EINTR
            return
          end

          ipaddr,port = addr[3],addr[1]
          if ipaddr != self.address || port != @port
            raise "udp ack heartbeat packet from unknown node #{ipaddr}:#{port}"
          end
          @state.update!
        end

        class StateMachine
          # if available
          #   failure_time passed without any successful updates -> unavailable
          #     (updates: successful heartbeatings, successful data transferring)
          # if unavailable
          #   recover_time passed with continuous updates -> available
          #     (continuous updates: all of update blank are shorter than failure_time)

          # heartbeat none & keepalive -> none of this class's business

          def initialize(failure_time, recover_time)
            @failure_time = failure_time
            @recover_time = recover_time
            @available = true
          end

          def available? ; @available ; end

          def start
            @status_changed_at = @success_at = Time.now
          end

          def check! # return true when status changed
            t = Time.now

            if @available
              if t > @success_at + @failure_time
                @available = false
                @status_changed_at = t
                return true
              end
            else
              if t < @success_at + @failure_time && t > @status_changed_at + @recover_time
                @available = true
                @status_changed_at = t
                return true
              elsif t > @success_at + @failure_time
                @status_changed_at = t
              end
            end
          end

          def update!
            @success_at = Time.now
          end
        end
      end
    end
  end
end
