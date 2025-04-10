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

require 'fluent/plugin_helper/thread'
require 'fluent/plugin_helper/server' # For Server::ServerTransportParams
require 'fluent/plugin_helper/http_server/server'
require 'fluent/plugin_helper/http_server/ssl_context_builder'

module Fluent
  module PluginHelper
    module HttpServer
      include Fluent::PluginHelper::Thread
      include Fluent::Configurable

      # stop     : stop http server and mark callback thread as stopped
      # shutdown : [-]
      # close    : correct stopped threads
      # terminate: kill thread

      def self.included(mod)
        mod.include Fluent::PluginHelper::Server::ServerTransportParams
      end

      def initialize(*)
        super
        @_http_server = nil
      end

      def create_http_server(title, addr:, port:, logger:, default_app: nil, proto: nil, tls_opts: nil, &block)
        logger.warn('this method is deprecated. Use #http_server_create_http_server instead')
        http_server_create_http_server(title, addr: addr, port: port, logger: logger, default_app: default_app, proto: proto, tls_opts: tls_opts, &block)
      end

      # @param title [Symbol] the thread name. this value should be unique.
      # @param addr [String] Listen address
      # @param port [String] Listen port
      # @param logger [Logger] logger used in this server
      # @param default_app [Object] This method must have #call.
      # @param proto [Symbol] :tls or :tcp
      # @param tls_opts [Hash] options for TLS.
      def http_server_create_http_server(title, addr:, port:, logger:, default_app: nil, proto: nil, tls_opts: nil, &block)
        unless block_given?
          raise ArgumentError, 'BUG: callback not specified'
        end

        if proto == :tls || (@transport_config && @transport_config.protocol == :tls)
          http_server_create_https_server(title, addr: addr, port: port, logger: logger, default_app: default_app, tls_opts: tls_opts, &block)
        else
          @_http_server = HttpServer::Server.new(addr: addr, port: port, logger: logger, default_app: default_app) do |serv|
            yield(serv)
          end

          _block_until_http_server_start do |notify|
            thread_create(title) do
              @_http_server.start(notify)
            end
          end
        end
      end

      # @param title [Symbol] the thread name. this value should be unique.
      # @param addr [String] Listen address
      # @param port [String] Listen port
      # @param logger [Logger] logger used in this server
      # @param default_app [Object] This method must have #call.
      # @param tls_opts [Hash] options for TLS.
      def http_server_create_https_server(title, addr:, port:, logger:, default_app: nil, tls_opts: nil)
        topt =
          if tls_opts
            _http_server_overwrite_config(@transport_config, tls_opts)
          else
            @transport_config
          end
        ctx = Fluent::PluginHelper::HttpServer::SSLContextBuilder.new($log).build(topt)

        @_http_server = HttpServer::Server.new(addr: addr, port: port, logger: logger, default_app: default_app, tls_context: ctx) do |serv|
          yield(serv)
        end

        _block_until_http_server_start do |notify|
          thread_create(title) do
            @_http_server.start(notify)
          end
        end
      end

      def stop
        if @_http_server
          @_http_server.stop
        end

        super
      end

      private

      def _http_server_overwrite_config(config, opts)
        conf = config.dup
        Fluent::PluginHelper::Server::SERVER_TRANSPORT_PARAMS.map(&:to_s).each do |param|
          if opts.key?(param)
            conf[param] = opts[param]
          end
        end
        conf
      end

      # To block until server is ready to listen
      def _block_until_http_server_start
        que = Queue.new
        yield(que)
        que.pop
      end
    end
  end
end
