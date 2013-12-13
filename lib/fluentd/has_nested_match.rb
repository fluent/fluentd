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
module Fluentd

  require 'fluentd/event_router'
  require 'fluentd/match_pattern'
  require 'fluentd/engine'
  require 'fluentd/collectors/null_collector'

  #
  # HasNestedMatch module is included by subclesses of Agent
  # so that it can have nested <match> elements.
  #
  # When <source> or other component emit events to
  # HasNestedMatch#collector (EventRouter class), it
  # forwards the events to a matched <match> element.
  #
  # Next step: 'fluentd/event_router.rb'
  #
  module HasNestedMatch
    def initialize
      super

      # HasNestedMatch logs a warning message if an emitted
      # record doesn't match any <match> elements
      default_collector = Collectors::NoMatchNoticeCollector.new(logger)
      # second argument (self) is EmitErrorHandler interface
      @event_router = EventRouter.new(default_collector, self)
    end

    attr_reader :event_router

    def configure(conf)
      super

      # initialize <match> elements
      conf.elements.select {|e|
        e.name == 'match'
      }.each {|e|
        pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
        type = e['type']
        add_match(type, pattern, e)
      }
    end

    def add_match(type, pattern, conf)
      log.info "adding match", pattern: pattern, type: type

      # PluginRegistry#new_output adds the created Output to @agents
      # (because Output is an Agent).
      output = Engine.plugins.new_output(self, type)
      output.configure(conf)

      @event_router.add_rule(pattern, output, copy: false)
      return output
    end

    # EmitErrorHandler interface. See 'fluentd/event_router.rb'
    def handle_emit_error(tag, time, record, error)
      log.warn "emit error", error: error
      log.debug_backtrace error.backtrace if error.respond_to?(:backtrace)
      if @root_agent
        @root_agent.error_collector.emit(tag, time, record)
      end
    end

    # EmitErrorHandler interface. See 'fluentd/event_router.rb'
    def handle_emits_error(tag, es, error)
      log.warn "emit error", error: error
      if @root_agent
        @root_agent.error_collector.emits(tag, es)
      end
    end
  end

end
