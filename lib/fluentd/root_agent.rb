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
  require_relative 'emitter_agent'
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
  # Message routing is implemented in EmitterAgent module. See also message_source.rb.
  #
  class RootAgent < EmitterAgent
    LOG_LABEL = "LOG".freeze
    ERROR_LABEL = "ERROR".freeze

    def initialize
      super

      @labels = {}

      @error_label = Collectors::NullCollector.new
      @log_label = Collectors::NullCollector.new

      # overwrite Agent#root_agent
      @root_agent = self

      self.default_collector = Collectors::NoMatchNoticeCollector.new
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

      @error_label = add_label_impl(ERROR_LABEL,
                     error_label_config, Collectors::ErrorNoticeCollector.new)

      @log_label = add_label_impl(LOG_LABEL,
                     log_label_config, Collectors::NullCollector.new)

      # override Fluentd::Logger#add_event
      Engine.log.extend(EventCollectLoggerMixin)
      Engine.log.root_agent = self

      nil
    end

    module EventCollectLoggerMixin
      def add_event(level, time, message, record, caller_stack)
        @root_agent.emit_log(time.to_i, message, record)
        super
      end

      attr_writer :root_agent
    end

    # root_router api
    def emit_error(tag, time, record)
      @error_label.collector.emit(tag, time, record)
    end

    # root_router api
    def emit_log(time, message, record)
      record = record.dup
      record['message'] = message
      @log_label.collector.emit("fluentd", time, record)
    end

    # root_router api
    def emits_label(label, tag, es)
      if label = @labels[label]
        return label.collector.emits(tag, es)
      else
        collector.emits(tag, es)
      end
    end

    def short_circuit_label(label, tag)
      if label = @labels[label]
        return label.collector.short_circuit(tag)
      else
        collector.short_circuit(tag)
      end
    end

    def add_label(label, e)
      # TODO validate label name

      add_label_impl(label, e, Collectors::NoMatchNoticeCollector.new)
      self
    end

    class Label < EmitterAgent
    end

    private

    def add_label_impl(label, e, default_collector)
      agent = Label.new
      agent.parent_agent = self
      agent.default_collector = default_collector

      agent.configure(e)

      @labels[label] = agent
    end

  end
end
