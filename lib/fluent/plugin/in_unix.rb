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

require 'fileutils'
require 'socket'

require 'cool.io'
require 'yajl'

require 'fluent/input'
require 'fluent/event'

module Fluent
  # obsolete
  class StreamInput < Input
    config_param :blocking_timeout, :time, default: 0.5

    def initialize
      require 'socket'
      require 'yajl'
      super
    end

    def start
      super

      @loop = Coolio::Loop.new
      @lsock = listen
      @loop.attach(@lsock)
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @loop.watchers.each {|w| w.detach }
      @loop.stop
      @lsock.close
      @thread.join

      super
    end

    #def listen
    #end

    def run
      @loop.run(@blocking_timeout)
    rescue
      log.error "unexpected error", error: $!.to_s
      log.error_backtrace
    end

    private

    # message Entry {
    #   1: long time
    #   2: object record
    # }
    #
    # message Forward {
    #   1: string tag
    #   2: list<Entry> entries
    # }
    #
    # message PackedForward {
    #   1: string tag
    #   2: raw entries  # msgpack stream of Entry
    # }
    #
    # message Message {
    #   1: string tag
    #   2: long? time
    #   3: object record
    # }
    def on_message(msg)
      # TODO format error
      tag = msg[0].to_s
      entries = msg[1]

      if entries.class == String
        # PackedForward
        es = MessagePackEventStream.new(entries)
        router.emit_stream(tag, es)

      elsif entries.class == Array
        # Forward
        es = MultiEventStream.new
        entries.each {|e|
          record = e[1]
          next if record.nil?
          time = e[0]
          time = (now ||= Engine.now) if time.to_i == 0
          es.add(time, record)
        }
        router.emit_stream(tag, es)

      else
        # Message
        record = msg[2]
        return if record.nil?

        time = msg[1]
        time = Engine.now if time.to_i == 0
        router.emit(tag, time, record)
      end
    end

    class Handler < Coolio::Socket
      def initialize(io, log, on_message)
        super(io)
        if io.is_a?(TCPSocket)
          opt = [1, @timeout.to_i].pack('I!I!')  # { int l_onoff; int l_linger; }
          io.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, opt)
        end
        @on_message = on_message
        @log = log
        @log.trace {
          remote_port, remote_addr = *Socket.unpack_sockaddr_in(@_io.getpeername) rescue nil
          "accepted fluent socket from '#{remote_addr}:#{remote_port}': object_id=#{self.object_id}"
        }
      end

      def on_connect
      end

      def on_read(data)
        first = data[0]
        if first == '{' || first == '['
          m = method(:on_read_json)
          @y = Yajl::Parser.new
          @y.on_parse_complete = @on_message
        else
          m = method(:on_read_msgpack)
          @u = Fluent::Engine.msgpack_factory.unpacker
        end

        singleton_class.module_eval do
          define_method(:on_read, m)
        end
        m.call(data)
      end

      def on_read_json(data)
        @y << data
      rescue
        @log.error "unexpected error", error: $!.to_s
        @log.error_backtrace
        close
      end

      def on_read_msgpack(data)
        @u.feed_each(data, &@on_message)
      rescue
        @log.error "unexpected error", error: $!.to_s
        @log.error_backtrace
        close
      end

      def on_close
        @log.trace { "closed fluent socket object_id=#{self.object_id}" }
      end
    end
  end

  class UnixInput < StreamInput
    Plugin.register_input('unix', self)

    desc 'The path to your Unix Domain Socket.'
    config_param :path, :string, default: DEFAULT_SOCKET_PATH
    desc 'The backlog of Unix Domain Socket.'
    config_param :backlog, :integer, default: nil

    def configure(conf)
      super
      #log.warn "'unix' input is obsoleted and will be removed. Use 'forward' instead."
    end

    def listen
      if File.exist?(@path)
        File.unlink(@path)
      end
      FileUtils.mkdir_p File.dirname(@path)
      log.info "listening fluent socket on #{@path}"
      s = Coolio::UNIXServer.new(@path, Handler, log, method(:on_message))
      s.listen(@backlog) unless @backlog.nil?
      s
    end
  end
end
