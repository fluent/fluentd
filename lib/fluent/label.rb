module Fluent
  require 'fluent/agent'

  class Label < Agent
    def initialize(name, opts = {})
      super(opts)

      @context = name
      @root_agent = nil
    end

    attr_accessor :root_agent

    def handle_emits_error(tag, es, e)
      @root_agent.handle_emits_error(tag, es, e)
    end
  end
end
