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
module Fluentd

  require 'delegate'

  require 'fluentd/agent'
  require 'fluentd/label'
  require 'fluentd/has_nested_match'
  require 'fluentd/collectors/null_collector'
  require 'fluentd/collectors/no_match_notice_collector'

  #
  # Fluentd forms a tree structure to manage plugins:
  #
  #                      RootAgent
  #                          |
  #             +------------+-------------+
  #             |            |             |
  #          <label>      <source>      <match>
  #             |                          |
  #        +----+----+                +----+----+
  #        |         |                |         |
  #     <match>   <match>       ( <match>    <match> )
  #                  |
  #             +---------+
  #             |         |
  #        ( <match>   <match> )
  #
  # Relation:
  # * RootAgent has many <label>, <source> and <match>
  # * <label>   has many <match>
  # * <match>   has many <match> (*)
  #
  # (*) <match> has nested <match> only if the output plugin extends
  #     FilteringOutput class or includes HasNestedMatch module.
  #
  # Next step: `fluentd/agent.rb`
  # Next step: 'fluentd/label.rb'
  # Next step: 'fluentd/has_nested_match.rb'
  #
  class RootAgent < Agent
    LOG_LABEL = "@LOG".freeze
    ERROR_LABEL = "@ERROR".freeze

    include HasNestedMatch

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
    end

    def configure(conf)
      # calls HasNestedMatch#configure and Agent#configure
      super

      error_label_config = nil
      log_label_config = nil

      # initialize <label> and <source> elements
      conf.elements.select {|e|
        e.name == 'label' || e.name == 'source'
      }.each {|e|
        case e.name
        when 'label'
          name = e.arg
          if name == ERROR_LABEL
            error_label_config = e
          elsif name == LOG_LABEL
            log_label_config = e
          else
            add_label(name, e)
          end

        when 'source'
          type = e['type']
          raise ConfigError, "Missing 'type' parameter" unless type
          add_source(type, e)
        end
      }

      # initialize built-in ERROR label
      error_label_config ||= conf.new_element("label", ERROR_LABEL)
      error_label = add_label_impl(ERROR_LABEL, RootAgentProxyWithoutErrorCollector.new(self), error_label_config)
      # overwrites HasNestedMatch#default_collector to show 'tag does not match' warnings
      error_label.event_router.default_collector = Collectors::NullCollector.new
      @error_collector = error_label.event_router

      # initialize built-in LOG label
      log_label_config ||= conf.new_element("label", LOG_LABEL)
      log_label = add_label_impl(LOG_LABEL, RootAgentProxyWithoutLogCollector.new(self), log_label_config)
      # overwrites HasNestedMatch#default_collector to show 'tag does not match' warnings
      log_label.event_router.default_collector = Collectors::NullCollector.new
      @log_collector = log_label.event_router

      nil
    end

    def add_source(type, conf)
      log.info "adding source", type: type

      input = Engine.plugins.new_input(self, type)

      # <source> emits events to the top-level event router (RootAgent#event_router).
      # Input#configure overwrites event_router to a label's event_router
      # if it has `to_label` parameter.
      # See also 'fluentd/plugin/input.rb'
      input.event_router = self.event_router

      input.configure(conf)

      return input
    end

    def add_label(name, e)
      add_label_impl(name, self, e)
      self
    end

    def add_label_impl(name, self_or_proxy, e)
      label = Label.new

      # overwrites Agent#parent_agent with a proxy so that
      # elements in the nested in <label @ERROR> or <label @LOG>
      # don't use RootAgent#log_collector or #error_collector.
      # otherwise it causes infinite loop.
      label.parent_agent = self_or_proxy

      label.configure(e)

      @labels[name] = label
    end

    private :add_label_impl

    # root_router api used by
    # 'fluentd/plugin/out_redirect.rb'
    def match_label(label_name, tag)
      if label = @labels[label_name]
        return label.event_router.match(tag)
      else
        self.event_router.default_collector
      end
    end

    def find_label(label_name)
      return @labels[label_name]
    end

    # See 'fluentd/agent_logger.rb'
    attr_reader :log_collector

    # See HasNestedMatch#handle_emit_error at
    # 'fluentd/has_nested_match.rb'
    attr_reader :error_collector

    # Agents nested in <label @LOG> element use RootAgent wrapped by
    # this RootAgentProxyWithoutLogCollector. So that those elements don't
    # send cause infinite loop.
    class RootAgentProxyWithoutLogCollector < SimpleDelegator
      def initialize(root_agent)
        super
        @log_collector = Collectors::NullCollector.new
      end

      # override #log_collector
      attr_reader :log_collector
    end

    # Agents nested in <label ERROR> element use RootAgent wrapped by
    # this RootAgentProxyWithoutErrorCollector. So that those elements don't
    # send cause infinite loop.
    class RootAgentProxyWithoutErrorCollector < SimpleDelegator
      def initialize(root_agent)
        super
        @error_collector = Collectors::NullCollector.new
      end

      # override #error_collector
      attr_reader :error_collector
    end
  end

end
