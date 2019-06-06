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

require 'fluent/plugin_helper/http/app'
require 'fluent/plugin_helper/http/router'
require 'fluent/plugin_helper/http/methods'

module Fluent
  module PluginHelper
    module Http
      class Server
        # @param default_app [Object] This method must have #call.
        def initialize(addr:, port:, logger:, default_app: nil)
          @addr = addr
          @port = port
          @logger = logger

          # TODO: support https and http2
          @uri = URI("http://#{@addr}:#{@port}").to_s
          @router = Router.new(default_app)
          @reactor = Async::Reactor.new
          @server = Async::HTTP::Server.new(
            App.new(@router, @logger),
            Async::HTTP::Endpoint.parse(@uri)
          )

          if block_given?
            yield(self)
          end
        end

        def start(notify = nil)
          @logger.debug("Start HTTP server listening #{@uri}")
          @reactor.run do
            @server.run

            if notify
              notify.push(:ready)
            end
          end
          @logger.debug('Finished HTTP server')
        end

        def stop
          @logger.debug('closing HTTP server')

          if @reactor
            @reactor.stop
          end
        end

        Http::Methods::ALL.map { |e| e.downcase.to_sym }.each do |name|
          define_method(name) do |path, app = nil, &block|

            unless path.start_with?('/')
              path += "/#{path}"
            end

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
