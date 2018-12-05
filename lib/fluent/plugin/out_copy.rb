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

require 'fluent/plugin/multi_output'
require 'fluent/config/error'
require 'fluent/event'

module Fluent::Plugin
  class CopyOutput < MultiOutput
    Fluent::Plugin.register_output('copy', self)

    desc 'If true, pass different record to each `store` plugin.'
    config_param :deep_copy, :bool, default: false

    def process(tag, es)
      unless es.repeatable?
        m = Fluent::MultiEventStream.new
        es.each {|time,record|
          m.add(time, record)
        }
        es = m
      end

      outputs.each do |output|
        output.emit_events(tag, @deep_copy ? es.dup : es)
      end
    end
  end
end
