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

  require 'delegate'

  require 'fluentd/agent'
  require 'fluentd/label'
  require 'fluentd/collectors/null_collector'
  require 'fluentd/collectors/no_match_notice_collector'
  require 'fluentd/collectors/error_notice_collector'

  #
  # Fluentd forms a tree structure to manage plugins:
  #
  #                      RootAgent
  #                          |
  #             +------------+-------------+
  #             |            |             |
  #          <label>      <label>       <source>
  #             |                         |
  #        +----+----+             +----+----+
  #        |         |             |         |
  #     <source>  <match>      <match>    <match>
  #                  |                       |
  #             +---------+               <match>
  #             |         |
  #          <match>   <match>
  #
  # Relation:
  # * RootAgent ---> <source>, <match>, <filter>, <label>
  # * <label>   ---> <source>, <match>, <filter>
  # * <source>, <match>, <filter> ---> <match>, <filter>
  #
  # Base class of RootAgent, <label>, <source>, <match> and <filter>
  # is Agent.
  #
  # Next step: `fluentd/agent.rb`
  #
  class RootAgent < Label
    LOG_LABEL = "LOG".freeze
    ERROR_LABEL = "ERROR".freeze

    def initialize
      super

      # overwrite Agent#root_agent and #parent_agent
      @root_agent = self
      @parent_agent = nil

      @labels = {}

      # built-in label for error stream
      @error_label = Collectors::NullCollector.new

      # built-in label for logging stream
      @log_label = Collectors::NullCollector.new

      # if an event doesn't even match top-level patterns,
      # NoMatchNoticeCollector shows warnings.
      self.default_collector = Collectors::NoMatchNoticeCollector.new
    end

    def configure(conf)
      super

      error_label_config = nil
      log_label_config = nil

      # initialize <label> elements
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

      # initialize built-in ERROR label
      error_label_config ||= conf.new_element("label", ERROR_LABEL)
      @error_label = add_label_impl(ERROR_LABEL, RootAgentProxyWithoutErrorLabel.new(self),
                     error_label_config, Collectors::ErrorNoticeCollector.new)

      # initialize built-in LOG label
      log_label_config ||= conf.new_element("label", LOG_LABEL)
      @log_label = add_label_impl(LOG_LABEL, RootAgentProxyWithoutLogLabel.new(self),
                     log_label_config, Collectors::NullCollector.new)

      # override Fluentd::Logger#add_event to send logs to LOG label
      Engine.log.extend(EventCollectLoggerMixin)
      Engine.log.root_agent = self

      nil
    end

    def add_label(name, e)
      # TODO validate label name

      add_label_impl(name, self, e, Collectors::NoMatchNoticeCollector.new)
      self
    end

    # root_router api used by
    # 'fluentd/collectors/label_redirect_collector.rb'
    def match_label(label_name, tag)
      if label = @labels[label_name]
        return label.collector.match(tag)
      else
        collector.match(tag)
      end
    end

    module EventCollectLoggerMixin
      attr_writer :root_agent
      def add_event(level, time, message, record, caller_stack)
        @root_agent.emit_log(time.to_i, message, record)
        super
      end
    end

    # root_router api
    def emit_log(time, message, record)
      record = record.dup
      record['message'] = message
      @log_label.collector.emit("fluentd", time, record)
    end

    class RootAgentProxyWithoutLogLabel < SimpleDelegator
      def emit_log(time, message, record)
        # do nothing to not cause infinite loop
      end
    end

    # root_router api
    def emit_error(tag, time, record)
      @error_label.collector.emit(tag, time, record)
    end

    class RootAgentProxyWithoutErrorLabel < SimpleDelegator
      def emit_error(tag, time, record)
        # do nothing to not cause infinite loop
      end
    end

    private

    def add_label_impl(name, self_or_proxy, e, default_collector)
      agent = Label.new
      agent.parent_agent = self_or_proxy
      agent.default_collector = default_collector

      agent.configure(e)

      @labels[name] = agent
    end

  end
end
