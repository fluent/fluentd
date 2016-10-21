module Fluent
  module Plugin
    class RegexpParser < Parser
      Plugin.register_parser("regexp", self)

      config_param :expression, :string
      config_param :ignorecase, :bool, default: false
      config_param :multiline, :bool, default: false

      config_set_default :time_key, 'time'

      def configure(conf)
        super

        expr = if @expression[0] == "/" && @expression[-1] == "/"
                 @expression[1..-2]
               else
                 @expression
               end
        regexp_option = 0
        regexp_option |= Regexp::IGNORECASE if @ignorecase
        regexp_option |= Regexp::MULTILINE if @multiline
        @regexp = Regexp.new(expr, regexp_option)
      end

      def parse(text)
        m = @regexp.match(text)
        unless m
          yield nil, nil
          return
        end

        record = {}
        m.names.each do |name|
          if value = m[name]
            record[name] = value
          end
        end

        yield convert_values(parse_time(record), record)
      end
    end
  end
end
