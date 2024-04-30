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

      def parse(data, &block)
        @unpacker.feed_each(data) do |obj|
          parse_unpacked_data(obj, &block)
        end
      end
      alias parse_partial_data parse

      def parse_io(io, &block)
        u = Fluent::MessagePackFactory.engine_factory.unpacker(io)
        u.each do |obj|
          parse_unpacked_data(obj, &block)
        end
      end

      def parse_unpacked_data(data)
        if data.is_a?(Hash)
          time, record = convert_values(parse_time(data), data)
          yield time, record
          return
        end

        unless data.is_a?(Array)
          yield nil, nil
          return
        end

        data.each do |record|
          unless record.is_a?(Hash)
            yield nil, nil
            next
          end
          time, converted_record = convert_values(parse_time(record), record)
          yield time, converted_record
        end
      end
    end
  end
end
