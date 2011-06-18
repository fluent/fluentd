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

  def initialize
    require 'rack'
    require 'webrick'
    @port = 9880
    @bind = '0.0.0.0'
  end

  def configure(conf)
    @port = conf['port'] || @port
    @port = @port.to_i

    @bind = conf['bind'] || @bind
  end

  def start
    case $log.level
    when Log::LEVEL_TRACE
      level = WEBrick::Log::DEBUG
    when Log::LEVEL_DEBUG
      level = WEBrick::Log::INFO
    else
      level = WEBrick::Log::WARN
    end

    log = WEBrick::Log.new($log.out, level)

    @server = ::WEBrick::HTTPServer.new({
      :BindAddress => @bind,
      :Port => @port,
      :Logger => log,
    })

    app = Rack::URLMap.new({
      '/' => method(:on_request),
    })
    @server.mount("/", ::Rack::Handler::WEBrick, app)

    @thread = Thread.new(&@server.method(:start))
  end

  def shutdown
    @server.shutdown
  end

  def on_request(env)
    request = ::Rack::Request.new(env)
    path_info = request.path_info[1..-1]  # remove '/'

    begin
      tag = path_info.split('/').join('.')

      if msgpack = request.POST['msgpack'] || request.GET['msgpack']
        record = MessagePack.unpack(msgpack)

      elsif js = request.POST['json'] || request.GET['json']
        record = JSON.parse(js)

      else
        raise "'json' or 'msgpack' parameter is required"
      end

      time = request.POST['time'] || request.GET['time']
      time = time.to_i
      if time == 0
        time = Engine.now
      end

      event = Event.new(time, record)

    rescue
      return [400, {'ContentType'=>'text/plain'}, "400 Bad Request\n#{$!}"]
    end

    Engine.emit(tag, event)
  end
end


end

