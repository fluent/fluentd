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
  class BufferOutput < Output
    Fluent::Plugin.register_output("buffer", self)
    helpers :event_emitter

    config_section :buffer do
      config_set_default :@type, "file"
      config_set_default :chunk_keys, ["tag"]
      config_set_default :flush_mode, :interval
      config_set_default :flush_interval, 10
    end

    def multi_workers_ready?
      true
    end

    def write(chunk)
      return if chunk.empty?
      router.emit_stream(chunk.metadata.tag, Fluent::MessagePackEventStream.new(chunk.read))
    end
  end
end
