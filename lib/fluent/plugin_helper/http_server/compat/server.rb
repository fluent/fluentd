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

require 'fluent/plugin_helper/http_server/methods'
require 'fluent/plugin_helper/http_server/compat/webrick_handler'
require 'fluent/plugin_helper/http_server/compat/ssl_context_extractor'

module Fluent
  module PluginHelper
    module HttpServer
      module Compat
        class Server
          # @param logger [Logger]
          # @param default_app [Object] ignored option. only for compat
          # @param tls_context [OpenSSL::SSL::SSLContext]
          def initialize(addr:, port:, logger:, default_app: nil, tls_context: nil)
            @addr = addr
            @port = port
            @logger = logger

            config = {
              BindAddress: @addr,
              Port: @port,
              Logger: WEBrick::Log.new(STDERR, WEBrick::Log::FATAL),
              AccessLog: [],
            }
            if tls_context
              require 'webrick/https'
              @logger.warn('Webrick ignores given TLS version')
              tls_opt = Fluent::PluginHelper::HttpServer::Compat::SSLContextExtractor.extract(tls_context)
              config = tls_opt.merge(**config)
            end

            @server = WEBrick::HTTPServer.new(config)

            # @example ["/example.json", :get, handler object]
            @methods = []

            if block_given?
              yield(self)
            end
          end

          def start(notify = nil)
            build_handler
            notify.push(:ready)
            @logger.debug('Start webrick HTTP server listening')
            @server.start
          end

          def stop
            @server.shutdown
            @server.stop
          end

          HttpServer::Methods::ALL.map { |e| e.downcase.to_sym }.each do |name|
            define_method(name) do |path, app = nil, &block|
              if (block && app) || (!block && !app)
                raise 'You must specify either app or block in the same time'
              end

              # Do not build a handler class here to able to handle multiple methods for single path.
              @methods << [path, name, app || block]
            end
          end

          private

          def build_handler
            @methods.group_by(&:first).each do |(path, rest)|
              klass = Fluent::PluginHelper::HttpServer::Compat::WebrickHandler.build(Hash[rest.map { |e| [e[1], e[2]] }])
              @server.mount(path, klass)
            end
          end
        end
      end
    end
  end
end
