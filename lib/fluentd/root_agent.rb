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

  #
  # RootAgent is the root node of Agent tree and also the root of MessageSource tree
  # (See agent.rb and message_source.rb).
  #
  # RootAgent reads config file and creates agents. Then registers them using into
  # the included MessageSource module.
  #
  class RootAgent < Agent
    include MessageSource

    LOG_LABEL = "LOG".freeze
    ERROR_LABEL = "ERROR".freeze

    def initialize
      super

      @labels = {}

      # init MessageSource
      init_message_source(self, Collectors::NoMatchNoticeCollector.new)

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

      add_label_impl(ErrorMessageLabel, ERROR_LABEL,
                     error_label_config, Collectors::ErrorNoticeCollector.new)

      log_agent = add_label_impl(LogMessageLabel, LOG_LABEL,
                     log_label_config, Collectors::NullCollector.new)

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
      agent.init_message_source(self, default_collector)

      agent.configure(e)
      add_agent(agent)  # Agent#add_agent

      @labels[label] = agent
    end

    class Label < Agent
      include MessageSource
    end

    class ErrorMessageLabel < Label
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
