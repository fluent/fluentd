require_relative 'helper'

module FluentTest
  class FluentTestOutput < ::Fluent::Output
    def initialize
      super
      @events = Hash.new { |h, k| h[k] = [] }
    end

    attr_reader :events

    def emit(tag, es, chain)
      es.each { |time, record|
        @events[tag] << record
      }
    end
  end

  class FluentTestErrorOutput < ::Fluent::BufferedOutput
    def format(tag, time, record)
      raise "emit error!"
    end

    def write(chunk)
      raise "chunk error!"
    end
  end

  class FluentTestFilter < ::Fluent::Filter
    def initialize(field = '__test__')
      super()
      @num = 0
      @field = field
    end

    attr_reader :num

    def filter(tag, time, record)
      record[@field] = @num
      @num += 1
      record
    end
  end

  class TestEmitErrorHandler
    def initialize
      @events = Hash.new { |h, k| h[k] = [] }
    end

    attr_reader :events

    def handle_emit_error(tag, time, record, error)
      @events[tag] << record
    end

    def handle_emits_error(tag, es, error)
      es.each { |time,record| handle_emit_error(tag, time, record, error) }
    end
  end
end
