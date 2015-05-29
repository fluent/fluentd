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

require 'fluent/plugin/buffer'
require 'fluent/plugin/buffer/memory_chunk'

module Fluent::Plugin
  class MemoryBuffer < Buffer
    Fluent::Plugin.register_buffer('memory', self)

    DEFAULT_CHUNK_BYTES_LIMIT = 8 * 1024 * 1024 # 8MB for memory
    DEFAULT_TOTAL_BYTES_LIMIT = 512 * 1024 * 1024 # 512MB

    def initialize
      super
    end

    config_section :buffer, param_name: :buffer_config do
      config_set_default :flush_at_shutdown, true
      config_set_default :chunk_bytes_limit, DEFAULT_CHUNK_BYTES_LIMIT
      config_set_default :total_bytes_limit, DEFAULT_TOTAL_BYTES_LIMIT
    end

    def configure(plugin_id, conf)
      super

      unless @flush_at_shutdown
        @log.warn "Confirm whether `flush_at_shutdown false` is correct or not. Chunk contents of memory buffer will be discarded at shutdown without flushing."
      end
    end

    def resume
      return {}, [] # stage, queue
    end

    def generate_chunk(metadata)
      Fluent::Plugin::Buffer::MemoryChunk.new(metadata)
    end
  end
end
