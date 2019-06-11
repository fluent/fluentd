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

require 'async/http/protocol'
require 'fluent/plugin_helper/http_server/methods'
require 'fluent/plugin_helper/http_server/request'

module Fluent
  module PluginHelper
    module HttpServer
      class App
        def initialize(router, logger)
          @logger = logger
          @router = router
        end

        # Required method by async-http
        def call(request)
          method = request.method
          resp =
            case method
            when HttpServer::Methods::GET
              get(request)
            when HttpServer::Methods::HEAD
              head(request)
            when HttpServer::Methods::POST
              post(request)
            when HttpServer::Methods::PATCH
              patch(request)
            when HttpServer::Methods::PUT
              put(request)
            when HttpServer::Methods::DELETE
              delete(request)
            when HttpServer::Methods::OPTIONS
              options(request)
            when HttpServer::Methods::CONNECT
              connect(request)
            when HttpServer::Methods::TRACE
              trace(request)
            end
          Protocol::HTTP::Response[*resp]
        rescue => e
          @logger.error(e)
          Protocol::HTTP::Response[500, { 'Content-Type' => 'text/http' }, 'Internal Server Error']
        end

        HttpServer::Methods::ALL.map { |e| e.downcase.to_sym }.each do |name|
          define_method(name) do |request|
            req = Request.new(request)

            path = req.path
            canonical_path =
              if path.size >= 2 && !path.end_with?('/')
                "#{path}/"
              else
                path
              end
            @router.route!(name, canonical_path, req)
          end
        end
      end
    end
  end
end
