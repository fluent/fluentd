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
require 'fluent/plugin'
require 'fluent/output'
require 'fluent/match'

module Fluent
  #
  # Agent is a resource unit who manages emittable plugins
  #
  # Next step: `fluentd/root_agent.rb`
  # Next step: `fluentd/label.rb`
  #
  class Agent
    include Configurable

    def initialize(log:)
      super()

      @context = nil
      @outputs = []
      @filters = []

      @lifecycle_control_list = nil
      # lifecycle_control_list is the list of plugins in this agent, and ordered
      # from plugins which DOES emit, then DOESN'T emit
      # (input -> output w/ router -> filter -> output w/o router)
      # for start: use this order DESC
      #   (because plugins which appears later in configurations will receive events from plugins which appears earlier)
      # for stop/before_shutdown/shutdown/after_shutdown/close/terminate: use this order ASC
      @lifecycle_cache = nil

      @log = log
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
      conf.elements('filter', 'match').each { |e|
        next if e.for_another_worker?
        pattern = e.arg.empty? ? '**' : e.arg
        type = e['@type']
        raise ConfigError, "Missing '@type' parameter on <#{e.name}> directive" unless type
        if e.name == 'filter'
          add_filter(type, pattern, e)
        else
          add_match(type, pattern, e)
        end
      }
    end

    def lifecycle_control_list
      return @lifecycle_control_list if @lifecycle_control_list

      lifecycle_control_list = {
        input: [],
        output_with_router: [],
        filter: [],
        output: [],
      }
      if self.respond_to?(:inputs)
        inputs.each do |i|
          lifecycle_control_list[:input] << i
        end
      end
      outputs.each do |o|
        if o.has_router?
          lifecycle_control_list[:output_with_router] << o
        else
          lifecycle_control_list[:output] << o
        end
      end
      filters.each do |f|
        lifecycle_control_list[:filter] << f
      end

      @lifecycle_control_list = lifecycle_control_list
    end

    def lifecycle(desc: false)
      kind_list = if desc
                    [:output, :filter, :output_with_router]
                  else
                    [:output_with_router, :filter, :output]
                  end
      kind_list.each do |kind|
        list = if desc
                 lifecycle_control_list[kind].reverse
               else
                 lifecycle_control_list[kind]
               end
        display_kind = (kind == :output_with_router ? :output : kind)
        list.each do |instance|
          yield instance, display_kind
        end
      end
    end

    def add_match(type, pattern, conf)
      log_type = conf.for_this_worker? ? :default : :worker0
      log.info log_type, "adding match#{@context.nil? ? '' : " in #{@context}"}", pattern: pattern, type: type

      output = Plugin.new_output(type)
      output.context_router = @event_router
      output.configure(conf)
      @outputs << output
      if output.respond_to?(:outputs) && output.respond_to?(:multi_output?) && output.multi_output?
        # TODO: ruby 2.3 or later: replace `output.respond_to?(:multi_output?) && output.multi_output?` with output&.multi_output?
        outputs = if output.respond_to?(:static_outputs)
                    output.static_outputs
                  else
                    output.outputs
                  end
        @outputs.push(*outputs)
      end
      @event_router.add_rule(pattern, output)

      output
    end

    def add_filter(type, pattern, conf)
      log_type = conf.for_this_worker? ? :default : :worker0
      log.info log_type, "adding filter#{@context.nil? ? '' : " in #{@context}"}", pattern: pattern, type: type

      filter = Plugin.new_filter(type)
      filter.context_router = @event_router
      filter.configure(conf)
      @filters << filter
      @event_router.add_rule(pattern, filter)

      filter
    end

    # For handling invalid record
    def emit_error_event(tag, time, record, error)
    end

    def handle_emits_error(tag, es, error)
    end
  end
end
