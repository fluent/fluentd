#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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

  #
  # MessageBus forms a tree structure whose root is a LabeledMessageBus:
  #
  #
  # MessageBus           MessageBus
  #   #parent_bus --+      #parent_bus --+
  #                 |                    |
  #                 +--> MessageBus      |
  # MessageBus      |      #parent_bus --+--> LabeledMessageBus
  #   #parent_bus --+                    |      #parent_bus == nil
  #                      MessageBus      |
  #                        #parent_bus --+
  #
  #
  # * MessageBus is an AgentGroup which manages many agents.
  # * MessageBus routes written records to one of the agents based on
  #   records' tags and match rules. (See MessageBus#open method).
  # * If none of agents match, MessageBus routes the record to #default_collector.
  # * #default_collector is usually #parent_bus.
  #

  class MessageBus
    include AgentGroup
    include Collector

    def initialize(parent_bus)
      super()
      @parent_bus = parent_bus
      @match_cache = []
      @match_cache_keys = []
      @matches = []
      if parent_bus
        @default_collector = parent_bus
      else
        @default_collector = Collectors::NoMatchCollector.new
      end
      @log = Logger.new(STDERR)  # TODO for test
    end

    MATCH_CACHE_SIZE = 1024

    attr_reader :parent_bus
    attr_accessor :default_collector

    def configure(conf)
      @log = conf.log

      conf.elements.select {|e|
        e.name == 'source' || e.name == 'match' || e.name == 'filter'
      }.each {|e|
        case e.name
        when 'source'
          type = e['type']
          raise ConfigError, "Missing 'type' parameter" unless type
          add_source(type, e)

        when 'match'
          pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
          type = e['type'] || 'redirect'
          add_match(type, pattern, e)

        when 'filter'
          pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
          type = e['type'] || 'redirect'
          add_filter(type, pattern, e)
        end
      }
    end

    def add_source(type, e)
      @log.info "adding source", :type=>type
      agent = e.plugin.new_input(type)

      bus = configure_agent(agent, e, self)
      bus.default_collector = OffsetMessageBus.new(self, @matches.size) if bus

      add_agent(agent)
    end

    def add_match(type, pattern, e)
      @log.info "adding match", :pattern=>pattern, :type=>type
      agent = e.plugin.new_output(type)

      bus = configure_agent(agent, e, self)
      bus.default_collector = Collectors::NoMatchCollector.new if bus

      # TODO check agent.class.is_a?(FilterOutput) and show warning

      add_agent(agent)
      @matches << Match.new(pattern, agent)
    end

    def add_filter(type, pattern, e)
      @log.info "adding filter", :pattern=>pattern, :type=>type
      agent = e.plugin.new_output(type)

      bus = configure_agent(agent, e, self)
      bus.default_collector = OffsetMessageBus.new(self, @matches.size+1) if bus

      # TODO check agent.class.is_a?(FilterOutput) and show warning

      add_agent(agent)
      @matches << Match.new(pattern, agent)
    end

    # override
    def open(tag, &block)
      open_offset(tag, 0, &block)
    end

    def open_offset(tag, offset, &block)
      cache = (@match_cache[offset] ||= {})
      collector = cache[tag]
      unless collector
        collector = match(tag, offset) || @default_collector
        keys = (@match_cache_keys[offset] ||= [])
        if keys.size > MATCH_CACHE_SIZE
          cache.delete keys.shift
        end
        cache[tag] = collector
        keys << tag
      end
      collector.open(tag, &block)
    end

    def open_label(label, tag, &block)
      @parent_bus.open_label(label, tag, &block)
    end

    private

    def match(tag, offset)
      @matches.each_with_index {|m,i|
        next if i < offset
        if m.pattern.match?(tag)
          return m.collector
        end
      }
      return nil
    end

    class Match
      def initialize(pattern, collector)
        @pattern = pattern
        @collector = collector
      end

      attr_reader :pattern, :collector
    end
  end


  class LabeledMessageBus < MessageBus
    def initialize
      super(nil)
      @labels = {}
    end

    def configure(conf)
      super

      conf.elements.select {|e|
        e.name == 'label'
      }.each {|e|
        add_label(e)
      }
    end

    # override
    def open_label(label, tag)
      return ensure_close(open(tag), &proc) if block_given?
      if bus = @labels[label]
        return bus.open(tag)
      else
        raise "unknown label: #{label}"  # TODO error class
      end
    end

    def add_label(e)
      label = e.arg  # TODO validate label

      bus = MessageBus.new(self)
      bus.configure(e)

      @labels[label] = bus

      add_agent_group(bus)
      self
    end
  end


  class OffsetMessageBus < DelegateClass(MessageBus)
    def initialize(bus, offset)
      super(bus)
      @offset = offset
    end

    # override
    def open(tag, &block)
      open_offset(tag, @offset, &block)
    end
  end
end

