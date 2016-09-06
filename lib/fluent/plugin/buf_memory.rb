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

module Fluent
  module Plugin
    class MemoryBuffer < Fluent::Plugin::Buffer
      Plugin.register_buffer('memory', self)

      def resume
        return {}, []
      end

      def generate_chunk(metadata)
        Fluent::Plugin::Buffer::MemoryChunk.new(metadata, compress: @compress)
      end
    end
  end
end
