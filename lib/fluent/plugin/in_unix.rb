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

require 'fluent/env'
require 'fluent/plugin/input'
require 'fluent/msgpack_factory'

require 'cool.io'
require 'yajl'
require 'fileutils'
require 'socket'

module Fluent::Plugin
  # TODO: This plugin will be 3rd party plugin
  class UnixInput < Input
    Fluent::Plugin.register_input('unix', self)

    helpers :event_loop

    def initialize
      super

      @lsock = nil
    end

    desc 'The path to your Unix Domain Socket.'
    config_param :path, :string, default: Fluent::DEFAULT_SOCKET_PATH
    desc 'The backlog of Unix Domain Socket.'
    config_param :backlog, :integer, default: nil
    desc "New tag instead of incoming tag"
    config_param :tag, :string, default: nil

    def configure(conf)
      super
    end

    def start
      super

      @lsock = listen
      event_loop_attach(@lsock)
    end

    def shutdown
      if @lsock
        event_loop_detach(@lsock)
        @lsock.close
      end

      super
    end

    def listen
      if File.exist?(@path)
        log.warn "Found existing '#{@path}'. Remove this file for in_unix plugin"
        File.unlink(@path)
      end
      FileUtils.mkdir_p(File.dirname(@path))

      log.info "listening fluent socket on #{@path}"
      s = Coolio::UNIXServer.new(@path, Handler, log, method(:on_message))
      s.listen(@backlog) unless @backlog.nil?
      s
    end

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
      unless msg.is_a?(Array)
        log.warn "incoming data is broken:", msg: msg
        return
      end

      tag = @tag || (msg[0].to_s)
      entries = msg[1]

      case entries
      when String
        # PackedForward
        es = Fluent::MessagePackEventStream.new(entries)
        router.emit_stream(tag, es)

      when Array
        # Forward
        es = Fluent::MultiEventStream.new
        entries.each {|e|
          record = e[1]
          next if record.nil?
          time = convert_time(e[0])
          es.add(time, record)
        }
        router.emit_stream(tag, es)

      else
        # Message
        record = msg[2]
        return if record.nil?

        time = convert_time(msg[1])
        router.emit(tag, time, record)
      end
    end

    def convert_time(time)
      case
      when time.nil? || (time == 0)
        Fluent::EventTime.now
      when time === Fluent::EventTime
        time
      else
        Fluent::EventTime.from_time(Time.at(time))
      end
    end

    class Handler < Coolio::Socket
      def initialize(io, log, on_message)
        super(io)

        @on_message = on_message
        @log = log
      end

      def on_connect
      end

      def on_read(data)
        first = data[0]
        if first == '{'.freeze || first == '['.freeze
          m = method(:on_read_json)
          @parser = Yajl::Parser.new
          @parser.on_parse_complete = @on_message
        else
          m = method(:on_read_msgpack)
          @parser = Fluent::MessagePackFactory.msgpack_unpacker
        end

        singleton_class.module_eval do
          define_method(:on_read, m)
        end
        m.call(data)
      end

      def on_read_json(data)
        @parser << data
      rescue => e
        @log.error "unexpected error in json payload", error: e.to_s
        @log.error_backtrace
        close
      end

      def on_read_msgpack(data)
        @parser.feed_each(data, &@on_message)
      rescue => e
        @log.error "unexpected error in msgpack payload", error: e.to_s
        @log.error_backtrace
        close
      end

      def on_close
        @log.trace { "closed fluent socket object_id=#{self.object_id}" }
      end
    end
  end
end
