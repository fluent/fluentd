require 'helper'

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

  class FluentTestFilter < ::Fluent::Filter
    def initialize
      super
      @num = 0
    end

    attr_reader :events

    def filter(tag, time, record)
      record['__test__'] = @num
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
