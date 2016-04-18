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
  module UniqueId
    def self.generate
      now = Time.now.utc
      u1 = ((now.to_i * 1000 * 1000 + now.usec) << 12 | rand(0xfff))
      [u1 >> 32, u1 & 0xffffffff, rand(0xffffffff), rand(0xffffffff)].pack('NNNN')
    end

    def self.hex(unique_id)
      unique_id.unpack('H*').first
    end

    module Mixin
      def generate_unique_id
        Fluent::UniqueId.generate
      end

      def dump_unique_id_hex(unique_id)
        Fluent::UniqueId.hex(unique_id)
      end
    end
  end
end
