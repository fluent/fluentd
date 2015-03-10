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

require 'fluent/plugin/input'
require 'fluent/process'
require 'fluent/plugin_support/tcp_server'

require 'http/parser'
require 'webrick/httputils'
require 'uri'

module Fluent::Plugin
  class HttpInput < Fluent::Plugin::Input
    include Fluent::DetachMultiProcessMixin
    include Fluent::PluginSupport::TCPServer

    Fluent::Plugin.register_input('http', self)

    EMPTY_GIF_IMAGE = "GIF89a\u0001\u0000\u0001\u0000\x80\xFF\u0000\xFF\xFF\xFF\u0000\u0000\u0000,\u0000\u0000\u0000\u0000\u0001\u0000\u0001\u0000\u0000\u0002\u0002D\u0001\u0000;".force_encoding("UTF-8")

    config_param :port, :integer, default: 9880
    config_param :bind, :string, default: '0.0.0.0'

    # request/response options
    config_param :format, :string, default: 'default' # HTTP content body format, or 'default'(query params)
    config_param :body_size_limit, :size, default: 32*1024*1024  # 32MB
    config_param :cors_allow_origins, :array, default: nil
    config_param :respond_with_empty_img, :bool, default: false

    # emit data options
    config_param :add_http_headers, :bool, default: false
    config_param :add_remote_addr, :bool, default: false

    # server options
    config_param :keepalive_timeout, :time, default: 10
    config_param :backlog, :integer, default: nil

    def configure(conf)
      super

      m = if @format == 'default'
            method(:parse_params_default)
          else
            @parser = Fluent::Plugin.new_parser(@format)
            @parser.configure(conf)
            method(:parse_params_with_parser)
          end
      (class << self; self; end).module_eval do
        define_method(:parse_params, m)
      end
    end

    def start
      super

      log.debug "listening http on #{@bind}:#{@port}"

      tcp_server_listen(port: @port, bind: @bind, keepalive: @keepalive_timeout, backlog: @backlog) do |conn_handler|
        http_parser_handler = HttpParserHandler.new(
          io_handler: conn_handler,
          callback: method(:on_request),
          format: format,
          body_size_limit: @body_size_limit,
          cors_allow_origins: @cors_allow_origins
        )
        http_parser = Http::Parser.new(http_parser_handler)
        http_parser_handler.http_parser = http_parser

        conn_handler.on_data do |data|
          http_parser << data
        end
      end
    end

    def shutdown
      super
    end

    def on_request(path_info, params)
      begin
        path = path_info[1..-1]  # remove /
        tag = path.split('/').join('.')
        record_time, record = parse_params(params)

        # Skip nil record
        if record.nil?
          if @respond_with_empty_img
            return ["200 OK", {'Content-type'=>'image/gif; charset=utf-8'}, EMPTY_GIF_IMAGE]
          else
            return ["200 OK", {'Content-type'=>'text/plain'}, ""]
          end
        end

        if @add_http_headers
          params.each_pair do |k,v|
            if k.start_with?("HTTP_")
              record[k] = v
            end
          end
        end

        if @add_remote_addr
          record['REMOTE_ADDR'] = params['REMOTE_ADDR']
        end

        time = if param_time = params['time']
                 param_time = param_time.to_i
                 param_time.zero? ? Fluent::Engine.now : param_time
               else
                 record_time.nil? ? Fluent::Engine.now : record_time
               end
      rescue => e
        # TODO: debug/trace logging
        return ["400 Bad Request", {'Content-type'=>'text/plain'}, "400 Bad Request\n#{$!}\n"]
      end

      # TODO server error
      begin
        # Support batched requests
        if record.is_a?(Array)
          mes = Fluent::MultiEventStream.new
          record.each do |single_record|
            single_time = single_record.delete("time") || time
            mes.add(single_time, single_record)
          end
          router.emit_stream(tag, mes)
        else
          router.emit(tag, time, record)
        end
      rescue
        return ["500 Internal Server Error", {'Content-type'=>'text/plain'}, "500 Internal Server Error\n#{$!}\n"]
      end

      if @respond_with_empty_img
        return ["200 OK", {'Content-type'=>'image/gif; charset=utf-8'}, EMPTY_GIF_IMAGE]
      else
        return ["200 OK", {'Content-type'=>'text/plain'}, ""]
      end
    end

    private

    def parse_params_default(params)
      record = if msgpack = params['msgpack']
                 MessagePack.unpack(msgpack)
               elsif js = params['json']
                 JSON.parse(js)
               else
                 raise "'json' or 'msgpack' parameter is required for 'default' format"
               end
      return nil, record
    end

    EVENT_RECORD_PARAMETER = '_event_record'

    def parse_params_with_parser(params)
      if content = params[EVENT_RECORD_PARAMETER]
        @parser.parse(content) do |time, record|
          raise "Received event is not #{@format}: #{content}" if record.nil?
          return time, record
        end
      else
        raise "'#{EVENT_RECORD_PARAMETER}' parameter is required"
      end
    end

    class HttpParserHandler
      attr_accessor :http_parser
      attr_reader :content_type, :origin

      def initialize(io_handler: nil, callback: ->(path_infor, params){}, body_size_limit: nil, format: 'default', cors_allow_origins: nil)
        @io_handler = io_handler
        @http_parser = nil # to be set after instanciation

        @callback = callback

        @body_size_limit = body_size_limit
        @format = format
        @cors_allow_origins = cors_allow_origins

        @closing = false
      end

      def on_message_begin
        @body = ''
        @env = {}
        @content_type = ''
        @origin = nil

        @closing = false
      end

      def on_headers_complete(headers)
        expect = nil
        size = nil

        if @http_parser.http_version == [1, 1] # TODO: HTTP/2.0
          @keep_alive = true
        else
          @keep_alive = false
        end
        headers.each_pair do |k,v|
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
          end
        end
        if expect
          if expect == '100-continue'
            if !size || size < @body_size_limit
              send_response("100 Continue", {}, nil)
            else
              send_response("413 Request Entity Too Large", {}, "Too large")
            end
          else
            send_response("417 Expectation Failed", {}, "")
          end
        end
      end

      def on_body(chunk)
        if @body.bytesize + chunk.bytesize > @body_size_limit
          unless @closing
            send_response("413 Request Entity Too Large", {}, "Too large")
          end
          return
        end
        @body << chunk
      end

      def on_message_complete
        return if @closing

        # CORS check
        # ==========
        # For every incoming request, we check if we have some CORS
        # restrictions and white listed origins through @cors_allow_origins.
        unless @cors_allow_origins.nil?
          unless @cors_allow_origins.include?(@origin)
            send_response("403 Forbidden", {'Connection' => 'close'}, "")
            return
          end
        end

        @env['REMOTE_ADDR'] = @io_handler.remote_addr if @io_handler.remote_addr

        uri = URI.parse(@http_parser.request_url)
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

        if @keep_alive
          header['Connection'] = 'Keep-Alive'
        end
        send_response(code, header, body)
      end

      def send_response(code, header, body=nil)
        unless @keep_alive
          @io_handler.closing = true # keepalive disabled for HTTP/1.0 (or earlier)
        end

        if body
          header['Content-length'] ||= body.bytesize
          header['Content-type'] ||= 'text/plain'
        end

        data = %[HTTP/1.1 #{code}\r\n]
        header.each_pair do |k,v|
          data << "#{k}: #{v}\r\n"
        end
        data << "\r\n"
        @io_handler.write data

        if body
          @io_handler.write body
        end
      end
    end
  end
end
