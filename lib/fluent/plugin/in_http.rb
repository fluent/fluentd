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

require 'uri'
require 'socket'
require 'json'

require 'cool.io'

require 'fluent/input'
require 'fluent/event'
require 'fluent/process'

module Fluent
  class HttpInput < Input
    Plugin.register_input('http', self)

    include DetachMultiProcessMixin

    require 'http/parser'

    def initialize
      require 'webrick/httputils'
      super
    end

    EMPTY_GIF_IMAGE = "GIF89a\u0001\u0000\u0001\u0000\x80\xFF\u0000\xFF\xFF\xFF\u0000\u0000\u0000,\u0000\u0000\u0000\u0000\u0001\u0000\u0001\u0000\u0000\u0002\u0002D\u0001\u0000;".force_encoding("UTF-8")

    desc 'The port to listen to.'
    config_param :port, :integer, default: 9880
    desc 'The bind address to listen to.'
    config_param :bind, :string, default: '0.0.0.0'
    desc 'The size limit of the POSTed element. Default is 32MB.'
    config_param :body_size_limit, :size, default: 32*1024*1024  # TODO default
    desc 'The timeout limit for keeping the connection alive.'
    config_param :keepalive_timeout, :time, default: 10   # TODO default
    config_param :backlog, :integer, default: nil
    desc 'Add HTTP_ prefix headers to the record.'
    config_param :add_http_headers, :bool, default: false
    desc 'Add REMOTE_ADDR header to the record.'
    config_param :add_remote_addr, :bool, default: false
    desc 'The format of the HTTP body.'
    config_param :format, :string, default: 'default'
    config_param :blocking_timeout, :time, default: 0.5
    desc 'Set a white list of domains that can do CORS (Cross-Origin Resource Sharing)'
    config_param :cors_allow_origins, :array, default: nil
    desc 'Respond with empty gif image of 1x1 pixel.'
    config_param :respond_with_empty_img, :bool, default: false

    def configure(conf)
      super

      m = if @format == 'default'
            method(:parse_params_default)
          else
            @parser = Plugin.new_parser(@format)
            @parser.configure(conf)
            method(:parse_params_with_parser)
          end
      (class << self; self; end).module_eval do
        define_method(:parse_params, m)
      end
    end

    class KeepaliveManager < Coolio::TimerWatcher
      def initialize(timeout)
        super(1, true)
        @cons = {}
        @timeout = timeout.to_i
      end

      def add(sock)
        @cons[sock] = sock
      end

      def delete(sock)
        @cons.delete(sock)
      end

      def on_timer
        @cons.each_pair {|sock,val|
          if sock.step_idle > @timeout
            sock.close
          end
        }
      end
    end

    def start
      log.debug "listening http on #{@bind}:#{@port}"

      socket_manager_path = ENV['SERVERENGINE_SOCKETMANAGER_PATH']
      if Fluent.windows?
        socket_manager_path = socket_manager_path.to_i
      end
      client = ServerEngine::SocketManager::Client.new(socket_manager_path)
      lsock = client.listen_tcp(@bind, @port)

      detach_multi_process do
        super
        @km = KeepaliveManager.new(@keepalive_timeout)
        @lsock = Coolio::TCPServer.new(lsock, nil, Handler, @km, method(:on_request),
                                       @body_size_limit, @format, log,
                                       @cors_allow_origins)
        @lsock.listen(@backlog) unless @backlog.nil?

        @loop = Coolio::Loop.new
        @loop.attach(@km)
        @loop.attach(@lsock)

        @thread = Thread.new(&method(:run))
      end
    end

    def shutdown
      @loop.watchers.each {|w| w.detach }
      @loop.stop
      @lsock.close
      @thread.join

      super
    end

    def run
      @loop.run(@blocking_timeout)
    rescue
      log.error "unexpected error", error: $!.to_s
      log.error_backtrace
    end

    def on_request(path_info, params)
      begin
        path = path_info[1..-1]  # remove /
        tag = path.split('/').join('.')
        record_time, record = parse_params(params)

        # Skip nil record
        if record.nil?
          if @respond_with_empty_img
            return ["200 OK", {'Content-Type'=>'image/gif; charset=utf-8'}, EMPTY_GIF_IMAGE]
          else
            return ["200 OK", {'Content-Type'=>'text/plain'}, ""]
          end
        end

        unless record.is_a?(Array)
          if @add_http_headers
            params.each_pair { |k,v|
              if k.start_with?("HTTP_")
                record[k] = v
              end
            }
          end
          if @add_remote_addr
            record['REMOTE_ADDR'] = params['REMOTE_ADDR']
          end
        end
        time = if param_time = params['time']
                 param_time = param_time.to_f
                 param_time.zero? ? Engine.now : Fluent::EventTime.from_time(Time.at(param_time))
               else
                 record_time.nil? ? Engine.now : record_time
               end
      rescue
        return ["400 Bad Request", {'Content-Type'=>'text/plain'}, "400 Bad Request\n#{$!}\n"]
      end

      # TODO server error
      begin
        # Support batched requests
        if record.is_a?(Array)
          mes = MultiEventStream.new
          record.each do |single_record|
            if @add_http_headers
              params.each_pair { |k,v|
                if k.start_with?("HTTP_")
                  single_record[k] = v
                end
              }
            end
            if @add_remote_addr
              single_record['REMOTE_ADDR'] = params['REMOTE_ADDR']
            end
            single_time = single_record.delete("time") || time
            mes.add(single_time, single_record)
          end
          router.emit_stream(tag, mes)
        else
          router.emit(tag, time, record)
        end
      rescue
        return ["500 Internal Server Error", {'Content-Type'=>'text/plain'}, "500 Internal Server Error\n#{$!}\n"]
      end

      if @respond_with_empty_img
        return ["200 OK", {'Content-Type'=>'image/gif; charset=utf-8'}, EMPTY_GIF_IMAGE]
      else
        return ["200 OK", {'Content-Type'=>'text/plain'}, ""]
      end
    end

    private

    def parse_params_default(params)
      record = if msgpack = params['msgpack']
                 Engine.msgpack_factory.unpacker.feed(msgpack).read
               elsif js = params['json']
                 JSON.parse(js)
               else
                 raise "'json' or 'msgpack' parameter is required"
               end
      return nil, record
    end

    EVENT_RECORD_PARAMETER = '_event_record'

    def parse_params_with_parser(params)
      if content = params[EVENT_RECORD_PARAMETER]
        @parser.parse(content) { |time, record|
          raise "Received event is not #{@format}: #{content}" if record.nil?
          return time, record
        }
      else
        raise "'#{EVENT_RECORD_PARAMETER}' parameter is required"
      end
    end

    class Handler < Coolio::Socket
      attr_reader :content_type

      def initialize(io, km, callback, body_size_limit, format, log, cors_allow_origins)
        super(io)
        @km = km
        @callback = callback
        @body_size_limit = body_size_limit
        @next_close = false
        @format = format
        @log = log
        @cors_allow_origins = cors_allow_origins
        @idle = 0
        @km.add(self)

        @remote_port, @remote_addr = *Socket.unpack_sockaddr_in(io.getpeername) rescue nil
      end

      def step_idle
        @idle += 1
      end

      def on_close
        @km.delete(self)
      end

      def on_connect
        @parser = Http::Parser.new(self)
      end

      def on_read(data)
        @idle = 0
        @parser << data
      rescue
        @log.warn "unexpected error", error: $!.to_s
        @log.warn_backtrace
        close
      end

      def on_message_begin
        @body = ''
      end

      def on_headers_complete(headers)
        expect = nil
        size = nil

        if @parser.http_version == [1, 1]
          @keep_alive = true
        else
          @keep_alive = false
        end
        @env = {}
        @content_type = ""
        headers.each_pair {|k,v|
          @env["HTTP_#{k.gsub('-','_').upcase}"] = v
          case k
          when /Expect/i
            expect = v
          when /Content-Length/i
            size = v.to_i
          when /Content-Type/i
            @content_type = v
          when /Connection/i
            if v =~ /close/i
              @keep_alive = false
            elsif v =~ /Keep-alive/i
              @keep_alive = true
            end
          when /Origin/i
            @origin  = v
          when /X-Forwarded-For/i
            @remote_addr = v.split(",").first
          end
        }
        if expect
          if expect == '100-continue'
            if !size || size < @body_size_limit
              send_response_nobody("100 Continue", {})
            else
              send_response_and_close("413 Request Entity Too Large", {}, "Too large")
            end
          else
            send_response_and_close("417 Expectation Failed", {}, "")
          end
        end
      end

      def on_body(chunk)
        if @body.bytesize + chunk.bytesize > @body_size_limit
          unless closing?
            send_response_and_close("413 Request Entity Too Large", {}, "Too large")
          end
          return
        end
        @body << chunk
      end

      def on_message_complete
        return if closing?

        # CORS check
        # ==========
        # For every incoming request, we check if we have some CORS
        # restrictions and white listed origins through @cors_allow_origins.
        unless @cors_allow_origins.nil?
          unless @cors_allow_origins.include?(@origin)
            send_response_and_close("403 Forbidden", {'Connection' => 'close'}, "")
            return
          end
        end

        @env['REMOTE_ADDR'] = @remote_addr if @remote_addr

        uri = URI.parse(@parser.request_url)
        params = WEBrick::HTTPUtils.parse_query(uri.query)

        if @format != 'default'
          params[EVENT_RECORD_PARAMETER] = @body
        elsif @content_type =~ /^application\/x-www-form-urlencoded/
          params.update WEBrick::HTTPUtils.parse_query(@body)
        elsif @content_type =~ /^multipart\/form-data; boundary=(.+)/
          boundary = WEBrick::HTTPUtils.dequote($1)
          params.update WEBrick::HTTPUtils.parse_form_data(@body, boundary)
        elsif @content_type =~ /^application\/json/
          params['json'] = @body
        end
        path_info = uri.path

        params.merge!(@env)
        @env.clear

        code, header, body = *@callback.call(path_info, params)
        body = body.to_s

        header['Access-Control-Allow-Origin'] = @origin if !@cors_allow_origins.nil? && @cors_allow_origins.include?(@origin)
        if @keep_alive
          header['Connection'] = 'Keep-Alive'
          send_response(code, header, body)
        else
          send_response_and_close(code, header, body)
        end
      end

      def on_write_complete
        close if @next_close
      end

      def send_response_and_close(code, header, body)
        send_response(code, header, body)
        @next_close = true
      end

      def closing?
        @next_close
      end

      def send_response(code, header, body)
        header['Content-Length'] ||= body.bytesize
        header['Content-Type'] ||= 'text/plain'

        data = %[HTTP/1.1 #{code}\r\n]
        header.each_pair {|k,v|
          data << "#{k}: #{v}\r\n"
        }
        data << "\r\n"
        write data

        write body
      end

      def send_response_nobody(code, header)
        data = %[HTTP/1.1 #{code}\r\n]
        header.each_pair {|k,v|
          data << "#{k}: #{v}\r\n"
        }
        data << "\r\n"
        write data
      end
    end
  end
end
