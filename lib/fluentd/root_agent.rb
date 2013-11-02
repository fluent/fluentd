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

      # error stream collector
      @error_collector = Collectors::NullCollector.new

      # internal log stream collector
      @log_collector = Collectors::NullCollector.new

      self.default_collector = Collectors::NullCollector.new
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
      error_label = add_label_impl(ERROR_LABEL, RootAgentProxyWIthoutErrorCollector.new(self), error_label_config)
      error_label.default_collector = Collectors::NullCollector.new
      @error_collector = error_label.collector  # overwrite @error_collector

      # initialize built-in LOG label
      log_label_config ||= conf.new_element("label", LOG_LABEL)
      log_label = add_label_impl(LOG_LABEL, RootAgentProxyWithoutLogCollector.new(self), log_label_config)
      log_label.default_collector = Collectors::NullCollector.new
      @log_collector = log_label.collector  # overwrite @log_collector

      nil
    end

    def add_label(name, e)
      # if an event doesn't even match top-level patterns,
      # NoMatchNoticeCollector shows warnings.
      add_label_impl(name, self, e, NoMatchNoticeCollector.new)
      self
    end

    # root_router api used by
    # 'fluentd/plugin/out_redirect.rb'
    def match_label(label_name, tag)
      if label = @labels[label_name]
        return label.collector.match(tag)
      else
        collector.match(tag)
      end
    end

    # See 'fluentd/agent_logger.rb'
    attr_reader :log_collector

    # See Agent#handle_emit_error at 'fluentd/agent.rb'
    attr_reader :error_collector

    # Agents nested in <label LOG> element use RootAgent wrapped by
    # this RootAgentProxyWithoutLogCollector. Thus those elements don't
    # send logs to @log_collector recursively.
    class RootAgentProxyWithoutLogCollector < SimpleDelegator
      def initialize(root_agent)
        super
        @log_collector = Collectors::NullCollector.new
      end

      # override #log_collector
      attr_reader :log_collector
    end

    # Agents nested in <label ERROR> element use RootAgent wrapped by
    # this RootAgentProxyWIthoutErrorCollector. Thus those elements don't
    # send logs to @error_collector recursively.
    class RootAgentProxyWIthoutErrorCollector < SimpleDelegator
      def initialize(root_agent)
        super
        @error_collector = Collectors::NullCollector.new
      end

      # override #error_collector
      attr_reader :error_collector
    end

    private

    def add_label_impl(name, self_or_proxy, e)
      label = Label.new
      label.parent_agent = self_or_proxy

      # if an event doesn't even match top-level patterns,
      # NoMatchNoticeCollector shows warnings.
      label.default_collector = Collectors::NoMatchNoticeCollector.new(label.logger)

      label.configure(e)

      @labels[name] = label
    end
  end

end
