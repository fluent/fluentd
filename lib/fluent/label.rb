module Fluent
  require 'fluent/agent'

  class Label < Agent
    def initialize(name, opts = {})
      super(opts)

      @context = name
      @root_agent = nil
    end

    attr_accessor :root_agent

    def emit_error_event(tag, time, record, e)
      @root_agent.emit_error_event(tag, time, record, e)
    end

    def handle_emits_error(tag, es, e)
      @root_agent.handle_emits_error(tag, es, e)
    end
  end
end
