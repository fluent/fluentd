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
module Fluent

  require 'delegate'

  require 'fluent/agent'
  require 'fluent/label'
  #require 'fluentd/collectors/null_collector'
  #require 'fluentd/collectors/no_match_notice_collector'

  #
  # Fluentd forms a tree structure to manage plugins:
  #
  #                      RootAgent
  #                          |
  #             +------------+-------------+
  #             |            |             |
  #          <label>      <source>      <match>
  #             |
  #        +----+----+
  #        |         |
  #     <filter>   <match>
  #
  # Relation:
  # * RootAgent has many <label>, <source> and <match>
  # * <label>   has many <match> and <filter>
  #
  # Next step: `fluentd/agent.rb`
  # Next step: 'fluentd/label.rb'
  #
  class RootAgent < Agent
    ERROR_LABEL = "@ERROR".freeze

    def initialize(opts = {})
      super

      @labels = {}
      @inputs = []
      @started_inputs = []
      @suppress_emit_error_log_interval = 0
      @next_emit_error_log_time = nil

      suppress_interval(opts[:suppress_interval]) if opts[:suppress_interval]
      @without_source = opts[:without_source] if opts[:without_source]
    end

    def configure(conf)
      super

      error_label_config = nil

      # initialize <label> elements
      label_configs = {}
      conf.elements.select { |e| e.name == 'label' }.each { |e|
        name = e.arg
        raise ConfigError, "Missing symbol argument on <label> directive" if name.empty?

        if name == ERROR_LABEL
          error_label_config = e
        else
          add_label(name, e)
          label_configs[name] = e
        end
      }
      # Call 'configure' here to avoid 'label not found'
      label_configs.each { |name, e| @labels[name].configure(e) }

      # initialize <source> elements
      if @without_source
        log.info "'--without-source' is applied. Ignore <source> sections"
      else
        conf.elements.select { |e| e.name == 'source' }.each { |e|
          type = e['type']
          raise ConfigError, "Missing 'type' parameter on <source> directive" unless type
          add_source(type, e)
        }
      end

      setup_error_label(error_label_config)
    end

    def setup_error_label(error_label_config)
      # initialize built-in ERROR label
      if error_label_config
        error_label = add_label(ERROR_LABEL, error_label_config)
        @error_collector = error_label.event_router
      end
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
            i.shutdown
          rescue => e
            log.warn "unexpected error while shutting down input plugin", :plugin => i.class, :plugin_id => i.plugin_id, :error_class => e.class, :error => e
            log.warn_backtrace
          end
        end
      }.each { |t| t.join }

      @labels.each { |n, l|
        l.shutdown
      }

      super
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

    def add_label(name, e)
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

    def handle_emits_error(tag, es, e)
      if @error_collector
        #@error_collector.emit_stream("error.#{tag}", e)
        @error_collector.emit_stream(tag, es)
      else
        now = Engine.now
        if @suppress_emit_error_log_interval == 0 || now > @next_emit_error_log_time
          log.warn "emit transaction failed ", :error_class => e.class, :error => e
          log.warn_backtrace
          # log.debug "current next_emit_error_log_time: #{Time.at(@next_emit_error_log_time)}"
          @next_emit_error_log_time = now + @suppress_emit_error_log_interval
          # log.debug "next emit failure log suppressed"
          # log.debug "next logged time is #{Time.at(@next_emit_error_log_time)}"
        end
        raise e
      end
    end
  end
end
