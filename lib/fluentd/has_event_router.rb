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

  #
  # HasEventRouter module is included by Input and HasNestedMatch.
  #
  # Next step: 'fluentd/has_nested_match.rb'
  #
  module HasEventRouter
    def initialize
      super

      # HasEventRouter logs a warning message if an emitted
      # record doesn't match any <match> elements
      default_collector = Collectors::NoMatchNoticeCollector.new(logger)
      # second argument (self) is EmitErrorHandler interface
      @event_router = EventRouter.new(default_collector, self)
    end

    attr_reader :event_router

    def default_collector=(collector)
      @event_router.default_collector = collector
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
