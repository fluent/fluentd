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
  module ExecUtil
    SUPPORTED_FORMAT = {
      'tsv' => :tsv,
      'json' => :json,
      'msgpack' => :msgpack,
    }

    class Parser
      def initialize(on_message)
        @on_message = on_message
      end
    end

    class TSVParser < Parser
      def initialize(keys, on_message)
        @keys = keys
        super(on_message)
      end

      def call(io)
        io.each_line(&method(:each_line))
      end

      def each_line(line)
        line.chomp!
        vals = line.split("\t")

        record = Hash[@keys.zip(vals)]

        @on_message.call(record)
      end
    end

    class JSONParser < Parser
      def call(io)
        y = Yajl::Parser.new
        y.on_parse_complete = @on_message
        y.parse(io)
      end
    end

    class MessagePackParser < Parser
      def call(io)
        @u = MessagePack::Unpacker.new(io)
        begin
          @u.each(&@on_message)
        rescue EOFError
          # ignore
        rescue IOError => e
          raise unless e.message == 'stream closed'
        end
      end
    end

    class Formatter
    end

    class TSVFormatter < Formatter
      def initialize(in_keys)
        @in_keys = in_keys
        super()
      end

      def call(record, out)
        last = @in_keys.length-1
        for i in 0..last
          key = @in_keys[i]
          out << record[key].to_s
          out << "\t" if i != last
        end
        out << "\n"
      end
    end

    class JSONFormatter < Formatter
      def call(record, out)
        out << Yajl.dump(record) << "\n"
      end
    end

    class MessagePackFormatter < Formatter
      def call(record, out)
        record.to_msgpack(out)
      end
    end
  end

  module Plugin
    ExecUtil = Fluent::ExecUtil
  end
end
