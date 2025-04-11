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

require 'cgi'
require 'async/http/protocol'
require 'fluent/plugin_helper/http_server/methods'

module Fluent
  module PluginHelper
    module HttpServer
      class Request
        attr_reader :path, :query_string

        def initialize(request)
          @request = request
          path = request.path
          @path, @query_string = path.split('?', 2)
        end

        def headers
          @request.headers
        end

        def query
          @query_string && CGI.parse(@query_string)
        end

        def body
          @request.body && @request.body.read
        end
      end
    end
  end
end
