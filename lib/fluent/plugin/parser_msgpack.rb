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

      def parser_type
        :binary
      end

      def parse(data, &block)
        Fluent::MessagePackFactory.engine_factory.unpacker.feed_each(data) do |obj|
          parse_unpacked_data(obj, &block)
        end
      end

      def parse_partial_data(data, &block)
        @unpacker ||= Fluent::MessagePackFactory.engine_factory.unpacker
        @unpacker.feed_each(data) do |obj|
          parse_unpacked_data(obj, skip_invalid: true, &block)
        end
      end

      def parse_io(io, &block)
        u = Fluent::MessagePackFactory.engine_factory.unpacker(io)
        u.each do |obj|
          parse_unpacked_data(obj, skip_invalid: true, &block)
        end
      end

      def parse_unpacked_data(data, skip_invalid: false)
        if data.is_a?(Hash)
          converted = convert_unpacked_record(data, skip_invalid)
          yield(*converted) if converted
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
          converted = convert_unpacked_record(record, skip_invalid)
          yield(*converted) if converted
        end
      end

      private

      def convert_unpacked_record(record, skip_invalid)
        convert_values(parse_time(record), record)
      rescue ParserError => e
        raise unless skip_invalid
        log&.warn "invalid record found", error: e
        nil
      end
    end
  end
end
