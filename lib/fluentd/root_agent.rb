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
  require_relative 'stats_collector'  # TODO file/class name

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

      # init EventEmitter
      init_event_emitter(self, Collectors::NoMatchNoticeCollector.new)

      # set Agent#stats_collector
      self.stats_collector = StatsCollector.new(self)
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

      add_label_impl(ErrorEventLabel, ERROR_LABEL,
                     error_label_config, Collectors::ErrorNoticeCollector.new)

      log_agent = add_label_impl(LogMessageLabel, LOG_LABEL,
                     log_label_config, Collectors::NullCollector.new)

      # hooks error logs to send them to the LogMessageLabel
      Fluentd.logger.extend(StatsCollectLoggerMixin)
      Fluentd.logger.init_stats_collect("fluentd", log_agent.collector)

      nil
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

    private

    def add_label_impl(klass, label, e, default_collector)
      agent = klass.new
      agent.init_event_emitter(self, default_collector)

      agent.configure(e)
      add_agent(agent)  # Agent#add_agent

      @labels[label] = agent
    end

    class Label < Agent
      include EventEmitter
    end

    class ErrorEventLabel < Label
      def init_stats_source(stats_collector)
        # prevent infinite loop
        c = SimpleDelegator.new(stats_collector)
        c.extend(NoErrorStatsCollectorMixin)
        super(c)
      end

      module NoErrorStatsCollectorMixin
        def emit_error(tag, time, record)
          # do nothing
        end
        def emits_error(tag, es)
          # do nothing
        end
      end
    end

    class LogMessageLabel < Label
      def configure(conf)
        # prevent infinite loop
        c = SimpleDelegator.new(conf)
        c.extend(NoStatsLoggerMixin)
        super(c)
      end

      # prevents infinite loop
      module NoStatsLoggerMixin
        def collect_stats(level, message, record, time)
        end
      end
    end

    module StatsCollectLoggerMixin
      def init_stats_collect(tag, collector)
        @tag = tag
        @collector = collector
      end

      def collect_stats(level, message, record, time)
        record['message'] = message
        tag = "#{@tag}.#{level}"
        @collector.emit(tag, time.to_i, record)
      end
    end
  end

end
