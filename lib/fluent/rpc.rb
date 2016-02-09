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

require 'webrick'

module Fluent
  module RPC
    class Server
      def initialize(endpoint, log)
        bind, port = endpoint.split(':')
        @bind = bind
        @port = port
        @log = log

        @server = WEBrick::HTTPServer.new(
          BindAddress: @bind,
          Port: @port,
          Logger: WEBrick::Log.new(STDERR, WEBrick::Log::FATAL),
          AccessLog: [],
        )
      end

      def mount(path, servlet, *args)
        @server.mount(path, servlet, *args)
        @log.debug "register #{path} RPC servlet"
      end

      def mount_proc(path, &block)
        @server.mount_proc(path) { |req, res|
          begin
            code, header, response = block.call(req, res)
          rescue => e
            @log.warn "failed to handle RPC request", path: path, error: e.to_s
            @log.warn_backtrace e.backtrace

            code = 500
            body = {
              'message '=> 'Internal Server Error',
              'error' => "#{e}",
              'backtrace'=> e.backtrace,
            }
          end

          code = 200 if code.nil?
          header = {'Content-Type' => 'application/json'} if header.nil?
          body = if response.nil?
                   '{"ok":true}'
                 else
                   response.body['ok'] = code == 200
                   response.body.to_json
                 end

          res.status = code
          header.each_pair { |k, v|
            res[k] = v
          }
          res.body = body
        }
        @log.debug "register #{path} RPC handler"
      end

      def start
        @log.debug "listening RPC http server on http://#{@bind}:#{@port}/"
        @thread = Thread.new {
          @server.start
        }
      end

      def shutdown
        if @server
          @server.shutdown
          @server = nil
        end
        if @thread
          @thread.join
          @thread = nil
        end
      end
    end
  end
end
