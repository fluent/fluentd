#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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

  require 'delegate'  # SimpleDelegator

  require_relative 'agent'
  require_relative 'event_emitter'
  require_relative 'collectors/null_collector'
  require_relative 'collectors/no_match_notice_collector'

  #
  # Fluentd forms a tree structure:
  #
  #                    Fluentd::Server
  #                     /           \
  #                    /             \
  #               Worker             Worker      --- for each <server> section
  #                  |                  |            in fluentd.conf
  #              RootAgent          RootAgent
  #                /   \               /  \
  #         Agent A     Agent B      ...  ...   --- <source>, <match>, <filter> or <label>
  #           /   \           \
  #    Agent C     Agent D     ...              --- <source>, <match> or <filter>
  #
  # Agent is the base class of <source> (input plugins), <match> (output plugins) and
  # <filter> (filter plugins).
  #
  # Worker is responsible to start/stop/shutdown all nested agents.
  # RootAgent initializes top-level agents, labels and built-in labels (LOG and ERROR)
  #
  # Message routing is implemented in EventEmitter module. See also message_source.rb.
  #
  class RootAgent < Agent
    include EventEmitter

    LOG_LABEL = "LOG".freeze
    ERROR_LABEL = "ERROR".freeze

    def initialize
      super

      @labels = {}

      @error_label = Collectors::NullCollector.new
      @log_label = Collectors::NullCollector.new

      # init EventEmitter
      init_event_emitter(self, Collectors::NoMatchNoticeCollector.new)
    end

    def emit_error(tag, time, record)
      @error_label.collector.emit(tag, time, record)
    end

    def emit_log(time, message, record)
      record = record.dup
      record['message'] = message
      @log_label.collector.emit("fluentd", time, record)
    end

    def configure(conf)
      super

      error_label_config = conf.new_element("label", ERROR_LABEL)
      log_label_config = conf.new_element("label", LOG_LABEL)

      conf.elements.select {|e|
        e.name == 'label'
      }.each {|e|
        case label = e.arg
        when ERROR_LABEL
          error_label_config = e
        when LOG_LABEL
          log_label_config = e
        else
          add_label(label, e)
        end
      }

      @error_label = add_label_impl(Label, ERROR_LABEL,
                     error_label_config, Collectors::ErrorNoticeCollector.new)

      @log_label = add_label_impl(Label, LOG_LABEL,
                     log_label_config, Collectors::NullCollector.new)

      # override Fluentd::Logger#add_event
      Engine.log.extend(EventCollectLoggerMixin)

      nil
    end

    module EventCollectLoggerMixin
      def add_event(level, time, message, record, caller_stack)
        Engine.root_agent.emit_log(time.to_i, message, record)
        super
      end
    end

    # root_router api
    def emits_label(label, tag, es)
      if label = @labels[label]
        return label.collector.emits(tag, es)
      else
        @default_collector.emits(tag, es)  # NoMatchNoticeCollector
      end
    end

    def short_circuit_label(label, tag)
      if label = @labels[label]
        return label.collector.short_circuit(tag)
      else
        @default_collector.short_circuit(tag)
      end
    end

    def add_label(label, e)
      # TODO validate label name

      add_label_impl(Label, label, e, Collectors::NoMatchNoticeCollector.new)
      self
    end

    class Label < Agent
      include EventEmitter
    end

    private

    def add_label_impl(klass, label, e, default_collector)
      agent = klass.new
      agent.init_event_emitter(self, default_collector)

      agent.configure(e)
      add_agent(agent)  # Agent#add_agent

      @labels[label] = agent
    end

  end
end
