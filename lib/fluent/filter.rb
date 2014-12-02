module Fluent
  class Filter
    include Configurable
    include PluginId
    include PluginLoggerMixin

    attr_accessor :router

    def initialize
      super
    end

    def configure(conf)
      super

      if label_name = conf['@label']
        label = Engine.root_agent.find_label(label_name)
        @router = label.event_router
      end
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
        begin
          new_es.add(time, filter(tag, time, record))
        rescue => e
          router.emit_error_event(tag, time, record, e)
        end
      }
      new_es
    end
  end
end
