# frozen_string_literal: true

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
require 'fluent/plugin/parser'
require 'fluent/event'

require 'http/parser'
require 'webrick/httputils'
require 'uri'
require 'socket'
require 'json'

module Fluent::Plugin
  class InHttpParser < Parser
    Fluent::Plugin.register_parser('in_http', self)

    config_set_default :time_key, 'time'

    def configure(conf)
      super

      # if no time parser related parameters, use in_http's time convert rule
      @time_parser = if conf.has_key?('time_type') || conf.has_key?('time_format')
                       time_parser_create
                     else
                       nil
                     end
    end

    def parse(text)
      # this plugin is dummy implementation not to raise error
      yield nil, nil
    end

    def get_time_parser
      @time_parser
    end
  end

  class HttpInput < Input
    Fluent::Plugin.register_input('http', self)

    helpers :parser, :compat_parameters, :event_loop, :server

    EMPTY_GIF_IMAGE = (+"GIF89a\u0001\u0000\u0001\u0000\x80\xFF\u0000\xFF\xFF\xFF\u0000\u0000\u0000,\u0000\u0000\u0000\u0000\u0001\u0000\u0001\u0000\u0000\u0002\u0002D\u0001\u0000;").force_encoding("UTF-8")

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
    config_param :blocking_timeout, :time, default: 0.5
    desc 'Set a allow list of domains that can do CORS (Cross-Origin Resource Sharing)'
    config_param :cors_allow_origins, :array, default: nil
    desc 'Tells browsers whether to expose the response to frontend when the credentials mode is "include".'
    config_param :cors_allow_credentials, :bool, default: false
    desc 'Respond with empty gif image of 1x1 pixel.'
    config_param :respond_with_empty_img, :bool, default: false
    desc 'Respond status code with 204.'
    config_param :use_204_response, :bool, default: false
    desc 'Dump error log or not'
    config_param :dump_error_log, :bool, default: true
    desc 'Add QUERY_ prefix query params to record'
    config_param :add_query_params, :bool, default: false

    config_section :parse do
      config_set_default :@type, 'in_http'
    end

    EVENT_RECORD_PARAMETER = '_event_record'

    def initialize
      super

      @km = nil
      @format_name = nil
      @parser_time_key = nil

      # default parsers
      @parser_msgpack = nil
      @parser_json = nil
      @default_time_parser = nil
      @default_keep_time_key = nil
      @float_time_parser = nil

      # <parse> configured parser
      @custom_parser = nil
    end

    def configure(conf)
      compat_parameters_convert(conf, :parser)

      super

      if @cors_allow_credentials
        if @cors_allow_origins.nil? || @cors_allow_origins.include?('*')
          raise Fluent::ConfigError, "Cannot enable cors_allow_credentials without specific origins"
        end
      end

      m = if @parser_configs.first['@type'] == 'in_http'
            @parser_msgpack = parser_create(usage: 'parser_in_http_msgpack', type: 'msgpack')
            @parser_msgpack.time_key = nil
            @parser_msgpack.estimate_current_event = false
            @parser_json = parser_create(usage: 'parser_in_http_json', type: 'json')
            @parser_json.time_key = nil
            @parser_json.estimate_current_event = false

            default_parser = parser_create(usage: '')
            @format_name = 'default'
            @parser_time_key = default_parser.time_key
            @default_time_parser = default_parser.get_time_parser
            @default_keep_time_key = default_parser.keep_time_key
            method(:parse_params_default)
          else
            @custom_parser = parser_create
            @format_name = @parser_configs.first['@type']
            @parser_time_key = @custom_parser.time_key
            method(:parse_params_with_parser)
          end
      self.singleton_class.module_eval do
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

    def multi_workers_ready?
      true
    end

    def start
      @_event_loop_run_timeout = @blocking_timeout

      super

      log.debug "listening http", bind: @bind, port: @port

      @km = KeepaliveManager.new(@keepalive_timeout)
      event_loop_attach(@km)

      server_create_connection(:in_http, @port, bind: @bind, backlog: @backlog, &method(:on_server_connect))
      @float_time_parser = Fluent::NumericTimeParser.new(:float)
    end

    def close
      server_wait_until_stop
      super
    end

    RES_TEXT_HEADER = {'Content-Type' => 'text/plain'}.freeze
    RESPONSE_200 = ["200 OK".freeze, RES_TEXT_HEADER, "".freeze].freeze
    RESPONSE_204 = ["204 No Content".freeze, {}.freeze].freeze
    RESPONSE_IMG = ["200 OK".freeze, {'Content-Type'=>'image/gif; charset=utf-8'}.freeze, EMPTY_GIF_IMAGE].freeze
    RES_400_STATUS = "400 Bad Request".freeze
    RES_500_STATUS = "500 Internal Server Error".freeze

    def on_request(path_info, params)
      begin
        path = path_info[1..-1]  # remove /
        tag = path.split('/').join('.')

        mes = Fluent::MultiEventStream.new
        parse_params(params) do |record_time, record|
          if record.nil?
            log.debug { "incoming event is invalid: path=#{path_info} params=#{params.to_json}" }
            next
          end

          add_params_to_record(record, params)

          time = if param_time = params['time']
                   param_time = param_time.to_f
                   param_time.zero? ? Fluent::EventTime.now : @float_time_parser.parse(param_time)
                 else
                   record_time.nil? ? convert_time_field(record) : record_time
                 end

          mes.add(time, record)
        end
      rescue => e
        if @dump_error_log
          log.error "failed to process request", error: e
        end
        return [RES_400_STATUS, RES_TEXT_HEADER, "400 Bad Request\n#{e}\n"]
      end

      # TODO server error
      begin
        router.emit_stream(tag, mes) unless mes.empty?
      rescue => e
        if @dump_error_log
          log.error "failed to emit data", error: e
        end
        return [RES_500_STATUS, RES_TEXT_HEADER, "500 Internal Server Error\n#{e}\n"]
      end

      if @respond_with_empty_img
        return RESPONSE_IMG
      else
        if @use_204_response
          return RESPONSE_204
        else
          return RESPONSE_200
        end
      end
    end

    private

    def on_server_connect(conn)
      handler = Handler.new(conn, @km, method(:on_request),
                            @body_size_limit, @format_name, log,
                            @cors_allow_origins, @cors_allow_credentials,
                            @add_query_params)

      conn.on(:data) do |data|
        handler.on_read(data)
      end

      conn.on(:write_complete) do |_|
        handler.on_write_complete
      end

      conn.on(:close) do |_|
        handler.on_close
      end
    end

    def parse_params_default(params)
      if msgpack = params['msgpack']
        @parser_msgpack.parse(msgpack) do |_time, record|
          yield nil, record
        end
      elsif js = params['json']
        @parser_json.parse(js) do |_time, record|
          yield nil, record
        end
      elsif ndjson = params['ndjson']
        ndjson.split(/\r?\n/).each do |js|
          @parser_json.parse(js) do |_time, record|
            yield nil, record
          end
        end
      else
        raise "'json', 'ndjson' or 'msgpack' parameter is required"
      end
    end

    def parse_params_with_parser(params)
      if content = params[EVENT_RECORD_PARAMETER]
        @custom_parser.parse(content) do |time, record|
          yield time, record
        end
      else
        raise "'#{EVENT_RECORD_PARAMETER}' parameter is required"
      end
    end

    def add_params_to_record(record, params)
      if @add_http_headers
        params.each_pair { |k, v|
          if k.start_with?("HTTP_".freeze)
            record[k] = v
          end
        }
      end

      if @add_query_params
        params.each_pair { |k, v|
          if k.start_with?("QUERY_".freeze)
            record[k] = v
          end
        }
      end

      if @add_remote_addr
        record['REMOTE_ADDR'] = params['REMOTE_ADDR']
      end
    end

    def convert_time_field(record)
      if t = @default_keep_time_key ? record[@parser_time_key] : record.delete(@parser_time_key)
        if @default_time_parser
          @default_time_parser.parse(t)
        else
          Fluent::EventTime.from_time(Time.at(t))
        end
      else
        Fluent::EventTime.now
      end
    end

    class Handler
      attr_reader :content_type

      def initialize(io, km, callback, body_size_limit, format_name, log,
                     cors_allow_origins, cors_allow_credentials, add_query_params)
        @io = io
        @km = km
        @callback = callback
        @body_size_limit = body_size_limit
        @next_close = false
        @format_name = format_name
        @log = log
        @cors_allow_origins = cors_allow_origins
        @cors_allow_credentials = cors_allow_credentials
        @idle = 0
        @add_query_params = add_query_params
        @km.add(self)

        @remote_port, @remote_addr = io.remote_port, io.remote_addr
        @parser = Http::Parser.new(self)
      end

      def step_idle
        @idle += 1
      end

      def on_close
        @km.delete(self)
      end

      def on_read(data)
        @idle = 0
        @parser << data
      rescue
        @log.warn "unexpected error", error: $!.to_s
        @log.warn_backtrace
        @io.close
      end

      def on_message_begin
        @body = +''
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
        @content_encoding = ""
        headers.each_pair {|k,v|
          @env["HTTP_#{k.tr('-','_').upcase}"] = v
          case k
          when /\AExpect\z/i
            expect = v
          when /\AContent-Length\Z/i
            size = v.to_i
          when /\AContent-Type\Z/i
            @content_type = v
          when /\AContent-Encoding\Z/i
            @content_encoding = v
          when /\AConnection\Z/i
            if /close/i.match?(v)
              @keep_alive = false
            elsif /Keep-alive/i.match?(v)
              @keep_alive = true
            end
          when /\AOrigin\Z/i
            @origin  = v
          when /\AX-Forwarded-For\Z/i
            # For multiple X-Forwarded-For headers. Use first header value.
            v = v.first if v.is_a?(Array)
            @remote_addr = v.split(",").first
          when /\AAccess-Control-Request-Method\Z/i
            @access_control_request_method = v
          when /\AAccess-Control-Request-Headers\Z/i
            @access_control_request_headers = v
          end
        }
        if expect
          if expect == '100-continue'.freeze
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

      RES_200_STATUS = "200 OK".freeze
      RES_403_STATUS = "403 Forbidden".freeze

      # Azure App Service sends GET requests for health checking purpose.
      # Respond with `200 OK` to accommodate it.
      def handle_get_request
        return send_response_and_close(RES_200_STATUS, {}, "")
      end

      # Web browsers can send an OPTIONS request before performing POST
      # to check if cross-origin requests are supported.
      def handle_options_request
        # Is CORS enabled in the first place?
        if @cors_allow_origins.nil?
          return send_response_and_close(RES_403_STATUS, {}, "")
        end

        # in_http does not support HTTP methods except POST
        if @access_control_request_method != 'POST'
          return send_response_and_close(RES_403_STATUS, {}, "")
        end

        header = {
          "Access-Control-Allow-Methods" => "POST",
          "Access-Control-Allow-Headers" => @access_control_request_headers || "",
        }

        # Check the origin and send back a CORS response
        if @cors_allow_origins.include?('*')
          header["Access-Control-Allow-Origin"] = "*"
          send_response_and_close(RES_200_STATUS, header, "")
        elsif include_cors_allow_origin
          header["Access-Control-Allow-Origin"] = @origin
          if @cors_allow_credentials
            header["Access-Control-Allow-Credentials"] = true
          end
          send_response_and_close(RES_200_STATUS, header, "")
        else
          send_response_and_close(RES_403_STATUS, {}, "")
        end
      end

      def on_message_complete
        return if closing?

        if @parser.http_method == 'GET'.freeze
          return handle_get_request()
        end

        if @parser.http_method == 'OPTIONS'.freeze
          return handle_options_request()
        end

        # CORS check
        # ==========
        # For every incoming request, we check if we have some CORS
        # restrictions and allow listed origins through @cors_allow_origins.
        unless @cors_allow_origins.nil?
          unless @cors_allow_origins.include?('*') || include_cors_allow_origin
            send_response_and_close(RES_403_STATUS, {'Connection' => 'close'}, "")
            return
          end
        end

        # Content Encoding
        # =================
        # Decode payload according to the "Content-Encoding" header.
        # For now, we only support 'gzip' and 'deflate'.
        begin
          if @content_encoding == 'gzip'.freeze
            @body = Zlib::GzipReader.new(StringIO.new(@body)).read
          elsif @content_encoding == 'deflate'.freeze
            @body = Zlib::Inflate.inflate(@body)
          end
        rescue
          @log.warn 'fails to decode payload', error: $!.to_s
          send_response_and_close(RES_400_STATUS, {}, "")
          return
        end

        @env['REMOTE_ADDR'] = @remote_addr if @remote_addr

        uri = URI.parse(@parser.request_url)
        params = WEBrick::HTTPUtils.parse_query(uri.query)

        if @format_name != 'default'
          params[EVENT_RECORD_PARAMETER] = @body
        elsif /^application\/x-www-form-urlencoded/.match?(@content_type)
          params.update WEBrick::HTTPUtils.parse_query(@body)
        elsif @content_type =~ /^multipart\/form-data; boundary=(.+)/
          boundary = WEBrick::HTTPUtils.dequote($1)
          params.update WEBrick::HTTPUtils.parse_form_data(@body, boundary)
        elsif /^application\/json/.match?(@content_type)
          params['json'] = @body
        elsif /^application\/csp-report/.match?(@content_type)
          params['json'] = @body
        elsif /^application\/msgpack/.match?(@content_type)
          params['msgpack'] = @body
        elsif /^application\/x-ndjson/.match?(@content_type)
          params['ndjson'] = @body
        end
        path_info = uri.path

        if (@add_query_params)

          query_params = WEBrick::HTTPUtils.parse_query(uri.query)

          query_params.each_pair {|k,v|
            params["QUERY_#{k.tr('-','_').upcase}"] = v
          }
        end

        params.merge!(@env)

        @env.clear

        code, header, body = @callback.call(path_info, params)
        body = body.to_s
        header = header.dup if header.frozen?

        unless @cors_allow_origins.nil?
          if @cors_allow_origins.include?('*')
            header['Access-Control-Allow-Origin'] = '*'
          elsif include_cors_allow_origin
            header['Access-Control-Allow-Origin'] = @origin
            if @cors_allow_credentials
              header["Access-Control-Allow-Credentials"] = true
            end
          end
        end

        if @keep_alive
          header['Connection'] = 'Keep-Alive'.freeze
          send_response(code, header, body)
        else
          send_response_and_close(code, header, body)
        end
      end

      def close
        @io.close
      end

      def on_write_complete
        @io.close if @next_close
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
        header['Content-Type'] ||= 'text/plain'.freeze

        data = +"HTTP/1.1 #{code}\r\n"
        header.each_pair {|k,v|
          data << "#{k}: #{v}\r\n"
        }
        data << "\r\n".freeze
        @io.write(data)

        @io.write(body)
      end

      def send_response_nobody(code, header)
        data = +"HTTP/1.1 #{code}\r\n"
        header.each_pair {|k,v|
          data << "#{k}: #{v}\r\n"
        }
        data << "\r\n".freeze
        @io.write(data)
      end

      def include_cors_allow_origin
        if @origin.nil?
          return false
        end

        if @cors_allow_origins.include?(@origin)
          return true
        end
        filtered_cors_allow_origins = @cors_allow_origins.select {|origin| origin != ""}
        r = filtered_cors_allow_origins.find do |origin|
          (start_str, end_str) = origin.split("*", 2)
          @origin.start_with?(start_str) && @origin.end_with?(end_str)
        end

        !r.nil?
      end
    end
  end
end
