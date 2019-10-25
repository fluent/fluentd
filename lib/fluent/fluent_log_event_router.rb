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
  # DO NOT write any logic here
  class NullFluentLogEventRouter
    def start; end

    def stop; end

    def graceful_stop; end

    def emit_event; end
  end

  # This class is for handling fluentd's inner log
  # e.g. <label @FLUNT_LOG> section and <match fluent.**> section
  class FluentLogEventRouter < NullFluentLogEventRouter
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
        root_log_event_router = root_agent.event_router

        if Fluent::Log.event_tags.any? { |t| root_log_event_router.match?(t) }
          log_event_router = root_log_event_router

          unmatched_tags = Fluent::Log.event_tags.select { |t| !log_event_router.match?(t) }
          unless unmatched_tags.empty?
            $log.warn "match for some tags of log events are not defined (to be ignored)", tags: unmatched_tags
          end
        end
      end

      if log_event_router
        FluentLogEventRouter.new(log_event_router)
      else
        $log.debug('No fluent logger for internal event')
        NullFluentLogEventRouter.new
      end
    end

    STOP = :stop
    GRACEFUL_STOP = :graceful_stop

    # @param event_router [Fluent::EventRouter]
    def initialize(event_router)
      @event_router = event_router
      @thread = nil
      @graceful_stop = false
      @event_queue = Queue.new
    end

    def start
      @thread = Thread.new do
        $log.disable_events(Thread.current)

        loop do
          event = @event_queue.pop

          case event
          when GRACEFUL_STOP
            @graceful_stop = true
          when STOP
            break
          else
            begin
              tag, time, record = event
              @event_router.emit(tag, time, record)
            rescue => e
              # This $log.error doesn't emit log events, because of `$log.disable_events(Thread.current)` above
              $log.error "failed to emit fluentd's log event", tag: tag, event: record, error: e
            end
          end

          if @graceful_stop && @event_queue.empty?
            break
          end
        end
      end

      @thread.abort_on_exception = true
    end

    def stop
      @event_queue.push(STOP)
      # there is no problem calling Thread#join multiple times.
      @thread && @thread.join
    end

    def graceful_stop
      # to make sure to emit all log events into router, before shutting down
      @event_queue.push(GRACEFUL_STOP)
      @thread && @thread.join
    end

    def emit_event(event)
      @event_queue.push(event)
    end
  end
end
