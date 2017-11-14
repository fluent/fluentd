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

require 'delegate'

require 'fluent/config/error'
require 'fluent/agent'
require 'fluent/engine'
require 'fluent/label'
require 'fluent/plugin'
require 'fluent/system_config'

module Fluent
  #
  # Fluentd forms a tree structure to manage plugins:
  #
  #                      RootAgent
  #                          |
  #             +------------+-------------+-------------+
  #             |            |             |             |
  #          <label>      <source>      <filter>      <match>
  #             |
  #        +----+----+
  #        |         |
  #     <filter>   <match>
  #
  # Relation:
  # * RootAgent has many <label>, <source>, <filter> and <match>
  # * <label>   has many <match> and <filter>
  #
  # Next step: `fluentd/agent.rb`
  # Next step: 'fluentd/label.rb'
  #
  class RootAgent < Agent
    ERROR_LABEL = "@ERROR".freeze # @ERROR is built-in error label

    def initialize(system_config = SystemConfig.new)
      super

      @labels = {}
      @inputs = []
      @started_inputs = []
      @suppress_emit_error_log_interval = 0
      @next_emit_error_log_time = nil

      suppress_interval(system_config.emit_error_log_interval) unless system_config.emit_error_log_interval.nil?
      @without_source = system_config.without_source unless system_config.without_source.nil?
    end

    attr_reader :inputs
    attr_reader :labels

    def configure(conf)
      error_label_config = nil

      # initialize <label> elements before configuring all plugins to avoid 'label not found' in input, filter and output.
      label_configs = {}
      conf.elements.select { |e| e.name == 'label' }.each { |e|
        name = e.arg
        raise ConfigError, "Missing symbol argument on <label> directive" if name.empty?

        if name == ERROR_LABEL
          error_label_config = e
        else
          add_label(name)
          label_configs[name] = e
        end
      }
      # Call 'configure' here to avoid 'label not found'
      label_configs.each { |name, e| @labels[name].configure(e) }
      setup_error_label(error_label_config) if error_label_config

      super

      # initialize <source> elements
      if @without_source
        log.info "'--without-source' is applied. Ignore <source> sections"
      else
        conf.elements.select { |e| e.name == 'source' }.each { |e|
          type = e['@type'] || e['type']
          raise ConfigError, "Missing '@type' parameter on <source> directive" unless type
          add_source(type, e)
        }
      end
    end

    def setup_error_label(e)
      error_label = add_label(ERROR_LABEL)
      error_label.configure(e)
      error_label.root_agent = RootAgentProxyWithoutErrorCollector.new(self)
      @error_collector = error_label.event_router
    end

    def start
      super

      @labels.each { |n, l|
        l.start
      }

      @inputs.each { |i|
        i.start
        @started_inputs << i
      }
    end

    def shutdown
      # Shutdown Input plugin first to prevent emitting to terminated Output plugin
      @started_inputs.map { |i|
        Thread.new do
          begin
            log.info "shutting down input", type: Plugin.lookup_name_from_class(i.class), plugin_id: i.plugin_id
            i.shutdown
          rescue => e
            log.warn "unexpected error while shutting down input plugin", plugin: i.class, plugin_id: i.plugin_id, error_class: e.class, error: e
            log.warn_backtrace
          end
        end
      }.each { |t| t.join }

      @labels.each { |n, l|
        l.shutdown
      }

      super
    end

    def flush!
      super
      @labels.each{ |name, label| label.flush! }
    end

    def suppress_interval(interval_time)
      @suppress_emit_error_log_interval = interval_time
      @next_emit_error_log_time = Time.now.to_i
    end

    def add_source(type, conf)
      log.info "adding source", type: type

      input = Plugin.new_input(type)
      # <source> emits events to the top-level event router (RootAgent#event_router).
      # Input#configure overwrites event_router to a label's event_router if it has `@label` parameter.
      # See also 'fluentd/plugin/input.rb'
      input.router = @event_router
      input.configure(conf)
      @inputs << input

      input
    end

    def add_label(name)
      label = Label.new(name)
      label.root_agent = self
      @labels[name] = label
    end

    def find_label(label_name)
      if label = @labels[label_name]
        label
      else
        raise ArgumentError, "#{label_name} label not found"
      end
    end

    def emit_error_event(tag, time, record, error)
      error_info = {error_class: error.class, error: error.to_s, location: (error.backtrace ? error.backtrace.first : nil), tag: tag, time: time}
      if @error_collector
        # A record is not included in the logs because <@ERROR> handles it. This warn is for the notification
        log.warn "send an error event to @ERROR:", error_info
        @error_collector.emit(tag, time, record)
      else
        error_info[:record] = record
        log.warn "dump an error event:", error_info
      end
    end

    def handle_emits_error(tag, es, error)
      error_info = {error_class: error.class, error: error.to_s, location: (error.backtrace ? error.backtrace.first : nil), tag: tag}
      if @error_collector
        log.warn "send an error event stream to @ERROR:", error_info
        @error_collector.emit_stream(tag, es)
      else
        now = Engine.now
        if @suppress_emit_error_log_interval.zero? || now > @next_emit_error_log_time
          log.warn "emit transaction failed:", error_info
          log.warn_backtrace
          @next_emit_error_log_time = now + @suppress_emit_error_log_interval
        end
        raise error
      end
    end

    # <label @ERROR> element use RootAgent wrapped by # this RootAgentProxyWithoutErrorCollector.
    # So that those elements don't send cause infinite loop.
    class RootAgentProxyWithoutErrorCollector < SimpleDelegator
      def initialize(root_agent)
        super

        @suppress_emit_error_log_interval = 0
        @next_emit_error_log_time = nil

        interval_time = root_agent.instance_variable_get(:@suppress_emit_error_log_interval)
        suppress_interval(interval_time) unless interval_time.zero?
      end

      def emit_error_event(tag, time, record, error)
        error_info = {error_class: error.class, error: error.to_s, location: (error.backtrace ? error.backtrace.first : nil), tag: tag, time: time, record: record}
        log.warn "dump an error event in @ERROR:", error_info
      end

      def handle_emits_error(tag, es, e)
        now = Engine.now
        if @suppress_emit_error_log_interval.zero? || now > @next_emit_error_log_time
          log.warn "emit transaction failed in @ERROR:", error_class: e.class, error: e, tag: tag
          log.warn_backtrace
          @next_emit_error_log_time = now + @suppress_emit_error_log_interval
        end
        raise e
      end
    end
  end
end
