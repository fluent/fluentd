#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent


class HttpInput < Input
  Plugin.register_input('http', self)

  require 'evma_httpserver'

  def initialize
    require 'webrick/httputils'
    @port = 9880
    @bind = '0.0.0.0'
  end

  def configure(conf)
    @port = conf['port'] || @port
    @port = @port.to_i

    @bind = conf['bind'] || @bind
  end

  # TODO multithreading
  def start
    $log.debug "listening http on #{@bind}:#{@port}"
    callback = method(:on_request)
    EventMachine.start_server(@bind, @port, Handler) {|c|
      c.callback = callback
    }
  end

  def shutdown
    # TODO graceful shut-down
  end

  def on_request(path_info, params)
    begin
      path = path_info[1..-1]  # remove /
      tag = path.split('/').join('.')

      if msgpack = params['msgpack']
        record = MessagePack.unpack(msgpack)

      elsif js = params['json']
        record = JSON.parse(js)

      else
        raise "'json' or 'msgpack' parameter is required"
      end

      time = params['time']
      time = time.to_i
      if time == 0
        time = Engine.now
      end

      event = Event.new(time, record)

    rescue
      return [400, {'Content-type'=>'text/plain'}, "400 Bad Request\n#{$!}\n"]
    end

    # TODO server error
    begin
      Engine.emit(tag, event)
    rescue
      return [500, {'Content-type'=>'text/plain'}, "500 Internal Server Error\n#{$!}\n"]
    end

    return [200, {'Content-type'=>'text/plain'}, ""]
  end

  module Handler
    include EventMachine::HttpServer

    attr_accessor :callback

    def process_http_request
      params = WEBrick::HTTPUtils.parse_query(@http_query_string)
      if @http_content_type =~ /^application\/x-www-form-urlencoded/
        params.update WEBrick::HTTPUtils.parse_query(@http_post_content)
      elsif @http_content_type =~ /^multipart\/form-data; boundary=(.+)/
        boundary = WEBrick::HTTPUtils.dequote($1)
        params.update WEBrick::HTTPUtils.parse_form_data(@http_post_content, boundary)
      end

      resp = EventMachine::DelegatedHttpResponse.new(self)

      code, header, body = @callback.call(@http_path_info, params)

      resp.status = code
      resp.headers = header
      resp.content = body.to_s
      resp.send_response

      # Cool.io doesn't support thread pool
      #op = Proc.new {
      #  @callback.call(@http_path_info, params)
      #}
      #
      #sender = Proc.new {|(code,header,body)|
      #  resp.status = code
      #  resp.headers = header
      #  resp.content = body.to_s
      #  resp.send_response
      #}
      #
      #EventMachine.defer(op, sender)
    end
  end
end


end

