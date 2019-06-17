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

module Fluent
  module PluginHelper
    module HttpServer
      module Methods
        GET = 'GET'.freeze
        HEAD = 'HEAD'.freeze
        POST = 'POST'.freeze
        PUT = 'PUT'.freeze
        PATCH = 'PATCH'.freeze
        DELETE = 'DELETE'.freeze
        OPTIONS = 'OPTIONS'.freeze
        CONNECT = 'CONNECT'.freeze
        TRACE = 'TRACE'.freeze

        ALL = [GET, HEAD, POST, PUT, PATCH, DELETE, CONNECT, OPTIONS, TRACE].freeze
      end
    end
  end
end
