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

require 'async'
require 'async/http'
require 'async/http/endpoint'

require 'fluent/plugin_helper/http_server/app'
require 'fluent/plugin_helper/http_server/router'
require 'fluent/plugin_helper/http_server/methods'
require 'fluent/log/console_adapter'

module Fluent
  module PluginHelper
    module HttpServer
      class Server
        # @param logger [Logger]
        # @param default_app [Object] This method must have #call.
        # @param tls_context [OpenSSL::SSL::SSLContext]
        def initialize(addr:, port:, logger:, default_app: nil, tls_context: nil)
          @addr = addr
          @port = port
          @logger = logger

          # TODO: support http2
          scheme = tls_context ? 'https' : 'http'
          @uri = URI("#{scheme}://#{@addr}:#{@port}").to_s
          @router = Router.new(default_app)
          @server_task = nil
          Console.logger = Fluent::Log::ConsoleAdapter.wrap(@logger)

          opts = if tls_context
                   { ssl_context: tls_context }
                 else
                   {}
                 end
          @server = Async::HTTP::Server.new(App.new(@router, @logger), Async::HTTP::Endpoint.parse(@uri, **opts))

          if block_given?
            yield(self)
          end
        end

        def start(notify = nil)
          @logger.debug("Start async HTTP server listening #{@uri}")

          Async do |task|
            @server_task = task.async do
              @server.run
            end
            if notify
              notify.push(:ready)
            end
          end

          @logger.debug('Finished HTTP server')
        end

        def stop
          @logger.debug('closing HTTP server')

          if @server_task
            @server_task.stop
          end
        end

        HttpServer::Methods::ALL.map { |e| e.downcase.to_sym }.each do |name|
          define_method(name) do |path, app = nil, &block|
            unless path.end_with?('/')
              path << '/'
            end

            if (block && app) || (!block && !app)
              raise 'You must specify either app or block in the same time'
            end

            @router.mount(name, path, app || block)
          end
        end
      end
    end
  end
end
