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
      es.each { |time, record|
        filter(tag, time, record)
      }
      es
    end
  end
end
