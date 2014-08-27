module Fluent
  class Filter
    include Configurable
    include PluginId
    include PluginLoggerMixin

    def initialize
      super
    end

    def configure(conf)
      super
    end

    def start
    end

    def shutdown
    end

    def filter(tag, time, record)
    end

    def filter_stream(tag, es)
      new_es = MultiEventStream.new
      es.each { |time, record|
        new_es.add(time, filter(tag, time, record))
      }
      new_es
    end
  end
end
