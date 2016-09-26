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

require 'cool.io'
require 'fluent/msgpack_factory'

module Fluent
  module Counter
    class BaseSocket < Coolio::TCPSocket
      include Fluent::MessagePackFactory::Mixin

      def packed_write(data)
        write pack(data)
      end

      def on_read(data)
        msgpack_unpacker.feed_each(data) do |d|
          on_message d
        end
      end

      def on_message(data)
        raise NotImplementedError
      end

      private

      def pack(data)
        msgpack_packer.pack(data)
      end
    end
  end
end
