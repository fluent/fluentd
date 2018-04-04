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

require 'fluent/plugin/parser_regexp'

module Fluent
  module Plugin
    class NginxParser < RegexpParser
      Plugin.register_parser("nginx", self)

      config_set_default :expression, %q{/^(?<remote>\S*) (?<host>\S*) (?<user>\S*) \[(?<time>[^\]]*)\] "(?<method>\S+)(?: +(?<path>[^\"]*?)(?:\s+\S*)?)?" (?<code>\S*) (?<size>\S*)(?: "(?<referer>[^\"]*)" "(?<agent>[^\"]*)"(?:\s+(?<http_x_forwarded_for>\S+))?)?$/}
      config_set_default :time_format, "%d/%b/%Y:%H:%M:%S %z"
    end
  end
end
