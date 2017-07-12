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

require 'fluent/configurable'
require 'fluent/engine'
require 'fluent/plugin'
require 'fluent/output'

module Fluent
  #
  # Agent is a resource unit who manages emittable plugins
  #
  # Next step: `fluentd/root_agent.rb`
  # Next step: `fluentd/label.rb`
  #
  class Agent
    include Configurable

    def initialize(opts = {})
      super()

      @context = nil
      @outputs = []
      @filters = []
      @started_outputs = []
      @started_filters = []

      @outputs_for_log_event = []
      @filters_for_log_event = []
      @started_outputs_for_log_event = []
      @started_filters_for_log_event = []

      @log = Engine.log
      @event_router = EventRouter.new(NoMatchMatch.new(log), self)
      @error_collector = nil
    end

    attr_reader :log
    attr_reader :outputs
    attr_reader :filters
    attr_reader :context
    attr_reader :event_router
    attr_reader :error_collector

    def configure(conf)
      super

      # initialize <match> and <filter> elements
      conf.elements.select { |e| e.name == 'filter' || e.name == 'match' }.each { |e|
        pattern = e.arg.empty? ? '**' : e.arg
        type = e['@type'] || e['type']
        raise ConfigError, "Missing '@type' parameter on <#{e.name}> directive" unless type
        if e.name == 'filter'
          add_filter(type, pattern, e)
        else
          add_match(type, pattern, e)
        end
      }
    end

    def start
      @outputs.each { |o|
        o.start
        @started_outputs << o
        @started_outputs_for_log_event << o if @outputs_for_log_event.include?(o)
      }

      @filters.each { |f|
        f.start
        @started_filters << f
        @started_filters_for_log_event << f if @filters_for_log_event.include?(f)
      }
    end

    def shutdown
      (@started_filters - @started_filters_for_log_event).map { |f|
        Thread.new do
          begin
            log.info "shutting down filter#{@context.nil? ? '' : " in #{@context}"}", type: Plugin.lookup_name_from_class(f.class), plugin_id: f.plugin_id
            f.shutdown
          rescue => e
            log.warn "unexpected error while shutting down filter plugins", plugin: f.class, plugin_id: f.plugin_id, error_class: e.class, error: e
            log.warn_backtrace
          end
        end
      }.each { |t| t.join }

      # Output plugin as filter emits records at shutdown so emit problem still exist.
      # This problem will be resolved after actual filter mechanizm.
      (@started_outputs - @started_outputs_for_log_event).map { |o|
        Thread.new do
          begin
            log.info "shutting down output#{@context.nil? ? '' : " in #{@context}"}", type: Plugin.lookup_name_from_class(o.class), plugin_id: o.plugin_id
            o.shutdown
          rescue => e
            log.warn "unexpected error while shutting down output plugins", plugin: o.class, plugin_id: o.plugin_id, error_class: e.class, error: e
            log.warn_backtrace
          end
        end
      }.each { |t| t.join }

      ## execute callback from Engine to flush log event queue before shutting down corresponding filters and outputs
      yield if block_given?

      @started_filters_for_log_event.map { |f|
        Thread.new do
          begin
            log.info "shutting down filter#{@context.nil? ? '' : " in #{@context}"}", type: Plugin.lookup_name_from_class(f.class), plugin_id: f.plugin_id
            f.shutdown
          rescue => e
            log.warn "unexpected error while shutting down filter plugins", plugin: f.class, plugin_id: f.plugin_id, error_class: e.class, error: e
            log.warn_backtrace
          end
        end
      }.each { |t| t.join }

      # Output plugin as filter emits records at shutdown so emit problem still exist.
      # This problem will be resolved after actual filter mechanizm.
      @started_outputs_for_log_event.map { |o|
        Thread.new do
          begin
            log.info "shutting down output#{@context.nil? ? '' : " in #{@context}"}", type: Plugin.lookup_name_from_class(o.class), plugin_id: o.plugin_id
            o.shutdown
          rescue => e
            log.warn "unexpected error while shutting down output plugins", plugin: o.class, plugin_id: o.plugin_id, error_class: e.class, error: e
            log.warn_backtrace
          end
        end
      }.each { |t| t.join }
    end

    def flush!
      flush_recursive(@outputs)
    end

    def flush_recursive(array)
      array.each { |o|
        begin
          if o.is_a?(BufferedOutput)
            o.force_flush
          elsif o.is_a?(MultiOutput)
            flush_recursive(o.outputs)
          end
        rescue => e
          log.debug "error while force flushing", error_class: e.class, error: e
          log.debug_backtrace
        end
      }
    end

    def add_match(type, pattern, conf)
      log.info "adding match#{@context.nil? ? '' : " in #{@context}"}", pattern: pattern, type: type

      output = Plugin.new_output(type)
      output.router = @event_router
      output.configure(conf)
      @outputs << output
      @event_router.add_rule(pattern, output)

      if match_event_log_tag?(pattern)
        @outputs_for_log_event << output
      end

      output
    end

    def add_filter(type, pattern, conf)
      log.info "adding filter#{@context.nil? ? '' : " in #{@context}"}", pattern: pattern, type: type

      filter = Plugin.new_filter(type)
      filter.router = @event_router
      filter.configure(conf)
      @filters << filter
      @event_router.add_rule(pattern, filter)

      if match_event_log_tag?(pattern)
        @filters_for_log_event << filter
      end

      filter
    end

    def match_event_log_tag?(pattern)
      EventRouter::Rule.new(pattern, nil).match?($log.tag)
    end

    # For handling invalid record
    def emit_error_event(tag, time, record, error)
    end

    def handle_emits_error(tag, es, error)
    end

    class NoMatchMatch
      def initialize(log)
        @log = log
        @count = 0
      end

      def emit(tag, es, chain)
        # TODO use time instead of num of records
        c = (@count += 1)
        if c < 512
          if Math.log(c) / Math.log(2) % 1.0 == 0
            @log.warn "no patterns matched", tag: tag
            return
          end
        else
          if c % 512 == 0
            @log.warn "no patterns matched", tag: tag
            return
          end
        end
        @log.on_trace { @log.trace "no patterns matched", tag: tag }
      end

      def start
      end

      def shutdown
      end
    end
  end
end
