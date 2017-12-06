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

require 'fluent/plugin/parser'

require 'fluent/time'

module Fluent
  module Plugin
    class NoneParser < Parser
      Plugin.register_parser('none', self)

      desc 'Field name to contain logs'
      config_param :message_key, :string, default: 'message'

      def parse(text)
        record = {}
        record[@message_key] = text
        time = @estimate_current_event ? Fluent::EventTime.now : nil
        yield time, record
      end
    end
  end
end
