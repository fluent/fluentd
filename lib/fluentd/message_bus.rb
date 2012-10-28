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


  class MessageBus
    include AgentGroup
    include Collector

    def initialize(parent_bus)
      super()
      @parent_bus = parent_bus
      @match_cache = {}
      @matches = []
      @default_collector = Collectors::NoMatchCollector.new
    end

    attr_accessor :parent_bus, :default_collector, :matches

    def configure(conf)
      @log = conf.logger

      conf.elements.select {|e|
        e.name == 'source' || e.name == 'match' || e.name == 'filter'
      }.each {|e|
        case e.name
        when 'source'
          type = e['type']
          raise ConfigError, "Missing 'type' parameter" unless type
          add_source(type, e)

        when 'filter'
          pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
          type = e['type']
          raise ConfigError, "Missing 'type' parameter" unless type
          add_filter(type, pattern, e)

        when 'match'
          pattern = MatchPattern.create(e.arg.empty? ? '**' : e.arg)
          type = e['type'] || 'redirect'
          add_output(type, pattern, e)
        end
      }
    end

    def add_source(type, e)
      @log.info "adding source", :type=>type
      agent = Plugin.new_input(type)
      configure_agent(agent, e, self)
      add_agent(agent)
    end

    def add_output(type, pattern, e)
      @log.info "adding match", :pattern=>pattern, :type=>type
      agent = Plugin.new_output(type)
      configure_agent(agent, e, self)
      add_agent(agent)

      @matches << Match.new(pattern, agent)
    end

    def add_filter(type, pattern, e)
      @log.info "adding filter", :pattern=>pattern, :type=>type
      agent = Plugin.new_filter(type)
      configure_agent(agent, e, self)
      add_agent(agent)

      @matches << FilterMatch.new(pattern, agent)
    end

    # override
    def open(tag, &block)
      collector = @match_cache[tag]
      unless collector
        collector = match(tag) || @default_collector
        if @match_cache.size < 1024  # TODO size limit
          @match_cache[tag] = collector
        end
      end
      collector.open(tag, &block)
    end

    def open_label(label, tag, &block)
      @parent_bus.open_label(label, tag, &block)
    end

    private

    def match(tag)
      collectors = []
      @matches.each {|m|
        if m.pattern.match?(tag)
          collectors << m.collector
          unless m.filter?
            if collectors.size == 1
              # optimization for this special case
              return collectors[0]
            else
              return Collectors::FilteringCollector.new(collectors)
            end
          end
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

      def filter?
        false
      end
    end

    class FilterMatch < Match
      def filter?
        true
      end
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
end

