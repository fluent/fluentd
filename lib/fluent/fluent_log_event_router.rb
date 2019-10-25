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

require 'fluent/log'

module Fluent
  # This class is for handling fluentd's inner log
  # e.g. <label @FLUNT_LOG> section and <match fluent.**> section
  class FluentLogEventRouter
    LOG_EMIT_INTERVAL = 0.1

    # @param root_agent [Fluent::RootAgent]
    def self.build(root_agent)
      log_event_router = nil

      begin
        log_event_agent = root_agent.find_label(Fluent::Log::LOG_EVENT_LABEL)
        log_event_router = log_event_agent.event_router

        # suppress mismatched tags only for <label @FLUENT_LOG> label.
        # it's not suppressed in default event router for non-log-event events
        log_event_router.suppress_missing_match!

        log_event_router = log_event_router

        unmatched_tags = Fluent::Log.event_tags.select { |t| !log_event_router.match?(t) }
        unless unmatched_tags.empty?
          $log.warn "match for some tags of log events are not defined (to be ignored)", tags: unmatched_tags
        end

      rescue ArgumentError # ArgumentError "#{label_name} label not found"
        # use default event router if <label @FLUENT_LOG> is missing in configuration
        log_event_router = root_agent.event_router

        if Fluent::Log.event_tags.any? { |t| log_event_router.match?(t) }
          log_event_router = log_event_router

          unmatched_tags = Fluent::Log.event_tags.select { |t| !log_event_router.match?(t) }
          unless unmatched_tags.empty?
            $log.warn "match for some tags of log events are not defined (to be ignored)", tags: unmatched_tags
          end
        end
      end

      if log_event_router
        FluentLogEventRouter.new(log_event_router)
      else
        nil # no need to create fluent log event emitter
      end
    end

    # @param event_router [Fluent::EventRouter]
    def initialize(event_router)
      @event_router = event_router
      @thread = nil
      @graceful_stop = false
      @stopped = false
      @event_queue = []
    end

    def start
      @thread = Thread.new do
        $log.disable_events(Thread.current)

        loop do
          sleep(LOG_EMIT_INTERVAL)

          break if @stop
          break if @graceful_stop && @event_queue.empty?

          next if @event_queue.empty?

          # NOTE: thead-safe of slice! depends on GVL
          events = @event_queue.slice!(0..-1)
          next if events.empty?

          events.each do |tag, time, record|
            begin
              @event_router.emit(tag, time, record)
            rescue => e
              # This $log.error doesn't emit log events, because of `$log.disable_events(Thread.current)` above
              $log.error "failed to emit fluentd's log event", tag: tag, event: record, error: e
            end
          end
        end
      end

      @thread.abort_on_exception = true
    end

    def stop
      @stop = true
      # there is no problem calling Thread#join multiple times.
      @thread.join
    end

    def graceful_stop
      # to make sure to emit all log events into router, before shutting down
      @graceful_stop = true
      @thread.join
    end

    def emit_event(event)
      @event_queue.push(event)
    end
  end
end
