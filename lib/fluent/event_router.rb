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
module Fluent
  require 'fluent/match'

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
  # 1) receive an event at `#emit` methods
  # 2) match the event's tag with the MatchPatterns
  # 3) forward the event to the corresponding Collector
  #
  # Collector is either of Output, Filter or other EventRouter.
  #
  class EventRouter
    def initialize(emit_error_handler, default_collector)
      @match_rules = []
      @match_cache = MatchCache.new
      @default_collector = default_collector
      @emit_error_handler = emit_error_handler || RaiseEmitErrorHandler.new
      @chain = NullOutputChain.instance
    end

    # Agent implements EmitErrorHandler. See 'fluentd/agent.rb'
    class RaiseEmitErrorHandler
      def handle_emits_error(tag, es, error)
        raise error
      end
    end

    attr_accessor :default_collector
    attr_accessor :emit_error_handler

    class Rule
      def initialize(pattern, collector)
        patterns = pattern.split(/\s+/).map { |str| MatchPattern.create(str) }
        @pattern = if patterns.length == 1
                     patterns[0]
                   else
                     OrMatchPattern.new(patterns)
                   end
        @pattern_str = pattern
        @collector = collector
      end

      def match?(tag)
        @pattern.match(tag)
      end

      attr_reader :collector
      attr_reader :patatern_str
    end

    # called by Agent to add new match pattern and collector
    def add_rule(pattern, collector)
      @match_rules << Rule.new(pattern, collector)
    end

    def emit(tag, time, record)
      unless record.nil?
        emit_stream(tag, OneEventStream.new(time, record))
      end
    end

    def emit_array(tag, array)
      emit_stream(tag, ArrayEventStream.new(array))
    end

    def emit_stream(tag, es)
      match(tag).emit(tag, es, @chain)
    rescue => e
      @emit_error_handler.handle_emits_error(tag, es, e)
    end

    def match?(tag)
      !!find(tag)
    end

    def match(tag)
      collector = @match_cache.get(tag) {
        c = find(tag) || @default_collector
      }
      collector
    end

    class MatchCache
      MATCH_CACHE_SIZE = 1024

      def initialize
        super
        @map = {}
        @keys = []
      end

      def get(key)
        if collector = @map[key]
          return collector
        end
        collector = @map[key] = yield
        if @keys.size >= MATCH_CACHE_SIZE
          # expire the oldest key
          @map.delete @keys.shift
        end
        @keys << key
        collector
      end
    end

    private

    def find(tag)
      @match_rules.each_with_index { |rule, i|
        if rule.match?(tag)
          return rule.collector
        end
      }
      nil
    end
  end
end
