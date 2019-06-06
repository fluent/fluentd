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
require 'fluent/plugin_helper/http'
require 'fluent/plugin_helper/http/methods'

module Fluent
  module PluginHelper
    module Http
      class Request
        attr_reader :path, :query_string

        def initialize(request)
          path = request.path
          @path, @query_string = path.split('?', 2)
        end

        def query
          CGI.parse(@query_string)
        end

        def body
          request.body
        end
      end
    end
  end
end
