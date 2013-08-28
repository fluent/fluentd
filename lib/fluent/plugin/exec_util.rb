module Fluent
  module ExecUtil
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
        end
      end
    end
  end
end


