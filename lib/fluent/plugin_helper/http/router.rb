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

module Fluent
  module PluginHelper
    module Http
      class Router
        class NotFoundApp
          def self.call(req)
            [404, { 'Content-Type' => 'text/plain' }, "404 Not Found: #{req.path}\n"]
          end
        end

        def initialize(default_app = nil)
          @router = { get: {}, head: {}, post: {}, put: {}, patch: {}, delete: {}, connect: {} }
          @default_app = default_app || NotFoundApp
        end

        # @param method [Symbol]
        # @param path [String]
        # @param app [Object]
        def mount(method, path, app)
          if @router[method].include?(path)
            raise "#{path} is already mounted"
          end

          @router[method][path] = app
        end

        # @param method [Symbol]
        # @param path [String]
        # @param request [Fluent::PluginHelper::Http::Request]
        def route!(method, path, request)
          @router.fetch(method).fetch(path, @default_app).call(request)
        end
      end
    end
  end
end
