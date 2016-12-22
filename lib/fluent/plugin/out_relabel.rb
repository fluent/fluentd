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

require 'fluent/plugin/output'

module Fluent::Plugin
  class RelabelOutput < Output
    Fluent::Plugin.register_output('relabel', self)
    helpers :event_emitter

    def multi_workers_ready?
      true
    end

    def process(tag, es)
      router.emit_stream(tag, es)
    end
  end
end
