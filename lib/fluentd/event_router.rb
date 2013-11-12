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

  require 'fluentd/collector'
  require 'fluentd/collectors/copy_collector'

  #
  # EventRouter is responsible to route events to a collector.
  #
  # It has a list of MatchPattern and Collector pairs:
  #
  #  +----------------+     +-----------------+
  #  |  MatchPattern  |     |    Collector    |
  #  +----------------+     +-----------------+
  #  |   access.**  ---------> type forward   |
  #  |     logs.**  ---------> type copy      |
  #  |  archive.**  ---------> type s3        |
  #  +----------------+     +-----------------+
  #
  # EventRouter does:
  #
  # 1) receive an event at `#emit` method
  # 2) match the event's tag with the MatchPatterns
  # 3) forward the event to the corresponding Collector
  #
  # Collector is either of Output, Filter, built-in collectors, (such as
  # fluentd/collectors/no_match_notice_collector.rb), or other EventRouter.
  #
  # If the destination is EventRouter, the EventRouter re-forwards the event
  # to another Collector. Eventually, the event reaches Output, Filter or
  # built-in collectors.
  #
  # NextStep: 'fluentd/plugin/output.rb'
  # NextStep: 'fluentd/plugin/input.rb'
  #
  class EventRouter
    include Collector

    def initialize(default_collector, emit_error_handler)
      @match_rules = []
      @match_caches = []
      @default_collector = default_collector
      @emit_error_handler = emit_error_handler || RaiseEmitErrorHandler.new
    end

    # Agent implements EmitErrorHandler. See 'fluentd/agent.rb'
    class RaiseEmitErrorHandler
      def handle_emit_error(tag, time, record, error)
        raise error
      end

      def handle_emits_error(tag, es, error)
        raise error
      end
    end

    attr_accessor :default_collector

    attr_accessor :emit_error_handler

    MATCH_CACHE_SIZE = 1024

    class Rule
      def initialize(pattern, collector, opts)
        @pattern = pattern
        @collector = collector
        @copy = opts[:copy]
      end

      def match?(tag)
        @pattern.match?(tag)
      end

      attr_reader :collector

      def copy?
        @copy
      end
    end

    # called by Agent to add new MatchPatterns and Collector
    def add_rule(pattern, collector, opts={})
      @match_rules << Rule.new(pattern, collector, opts)
      nil
    end

    # override Collector#emit for efficiency
    def emit(tag, time, record)
      match_offset(0, tag).emit(tag, time, record)
    rescue => e
      @emit_error_handler.handle_emit_error(tag, time, record, e)
      nil
    end

    # override Collector#emits
    def emits(tag, es)
      match_offset(0, tag).emits(tag, es)
    rescue => e
      @emit_error_handler.handle_emits_error(tag, es, e)
      nil
    end

    # override Collector#match
    def match(tag)
      match_offset(0, tag)
    end

    def current_offset
      Offset.new(self, @match_rules.size+1)
    end

    class Offset
      include Collector

      def initialize(router, offset)
        @router = router
        @offset = offset
      end

      def emit(tag, time, record)
        @router.emit_offset(@offset, tag, time, record)
      end

      def emits(tag, es)
        @router.emits_offset(@offset, tag, es)
      end

      def match(tag)
        @router.match_offset(@offset, tag)
      end
    end

    def emit_offset(offset, tag, time, record)
      match_offset(tag, offset).emit(tag, time, record)
    rescue => e
      @emit_error_handler.handle_emit_error(tag, time, record, e)
      nil
    end

    def emits_offset(offset, tag, es)
      match_offset(offset, tag).emits(tag, es)
    rescue => e
      @emit_error_handler.handle_emits_error(tag, es, e)
      nil
    end

    def match_offset(offset, tag)
      cache = (@match_caches[offset] ||= MatchCache.new)

      collector = cache.get(tag) do
        c = find(tag, offset) || @default_collector.match(tag)
      end

      return collector
    end

    class MatchCache
      def initialize
        super
        @map = {}
        @keys = []
      end

      def get(key, &find_block)
        if collector = @map[key]
          return collector
        end
        collector = @map[key] = find_block.call
        if @keys.size >= MATCH_CACHE_SIZE
          # expire the oldest key
          @map.delete @keys.shift
        end
        @keys << key
        return collector
      end
    end

    private

    def find(tag, offset)
      copy_collectors = nil
      @match_rules.each_with_index {|rule,i|
        next if i < offset
        if rule.match?(tag)
          if rule.copy?
            copy_collectors ||= []
            copy_collectors << rule.collector.match(tag)
          elsif copy_collectors
            copy_collectors << rule.collector.match(tag)
            return Collectors::CopyCollector.new(copy_collectors, emit_error_handler: @emit_error_handler)
          else
            return rule.collector.match(tag)
          end
        end
      }
      return nil
    end
  end

end
