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
require 'json'

module Fluent
  module PluginHelper
    module Http
      module Compat
        class WebrickHandler
          # **opt is enough. but I wrote a signature explicitly for readability
          def self.build(get: nil, head: nil, post: nil, put: nil, patch: nil, delete: nil, connect: nil)
            opt = { get: get, head: head, post: post, put: put, patch: patch, delete: delete, connect: connect }

            Class.new(WEBrick::HTTPServlet::AbstractServlet) do
              Http::Methods::ALL.each do |name|
                define_method("do_#{name}") do |request, response|
                  code, headers, body =
                                 if request.path_info != ''
                                   render_json(404, 'message' => 'Not found')
                                 else
                                   begin
                                     opt[name.downcase.to_sym].call(request)
                                   rescue => _
                                     render_json(500, 'message' => 'Something went wrong')
                                   end
                                 end

                  response.status = code
                  headers.each { |k, v| response[k] = v }
                  response.body = body
                end

                def render_json(code, obj)
                  [code, { 'Content-Type' => 'application/json' }, obj.to_json]
                end
              end
            end
          end
        end
      end
    end
  end
end
