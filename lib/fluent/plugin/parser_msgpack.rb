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
require 'fluent/msgpack_factory'

module Fluent
  module Plugin
    class MessagePackParser < Parser
      Plugin.register_parser('msgpack', self)

      def configure(conf)
        super
        @unpacker = Fluent::MessagePackFactory.engine_factory.unpacker
      end

      def parser_type
        :binary
      end

      def parse(data)
        @unpacker.feed_each(data) do |obj|
          yield convert_values(parse_time(obj), obj)
        end
      end
      alias parse_partial_data parse

      def parse_io(io, &block)
        u = Fluent::MessagePackFactory.engine_factory.unpacker(io)
        u.each do |obj|
          time, record = convert_values(parse_time(obj), obj)
          yield time, record
        end
      end
    end
  end
end
