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

require 'fluent/match'
require 'fluent/event'
require 'fluent/filter'

module Fluent
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
    def initialize(default_collector, emit_error_handler)
      @match_rules = []
      @match_cache = MatchCache.new
      @default_collector = default_collector
      @emit_error_handler = emit_error_handler
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
      attr_reader :pattern_str
    end

    def suppress_missing_match!
      if @default_collector.respond_to?(:suppress_missing_match!)
        @default_collector.suppress_missing_match!
      end
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
      match(tag).emit_events(tag, es)
    rescue => e
      @emit_error_handler.handle_emits_error(tag, es, e)
    end

    def emit_error_event(tag, time, record, error)
      @emit_error_handler.emit_error_event(tag, time, record, error)
    end

    def match?(tag)
      !!find(tag)
    end

    def match(tag)
      collector = @match_cache.get(tag) {
        find(tag) || @default_collector
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

    class Pipeline
      def initialize
        @filters = []
        @output = nil
        @optimizer = FilterOptimizer.new
      end

      def add_filter(filter)
        @filters << filter
        @optimizer.filters = @filters
      end

      def set_output(output)
        @output = output
      end

      def emit_events(tag, es)
        processed = @optimizer.filter_stream(tag, es)
        @output.emit_events(tag, processed)
      end

      class FilterOptimizer
        def initialize(filters = [])
          @filters = filters
          @optimizable = nil
        end

        def filters=(filters)
          @filters = filters
          reset_optimization
        end

        def filter_stream(tag, es)
          if optimizable?
            optimized_filter_stream(tag, es)
          else
            @filters.reduce(es) { |acc, filter| filter.filter_stream(tag, acc) }
          end
        end

        private

        def optimized_filter_stream(tag, es)
          new_es = MultiEventStream.new
          es.each do |time, record|
            filtered_record = record
            filtered_time = time

            catch :break_loop do
              @filters.each do |filter|
                if filter.has_filter_with_time
                  begin
                    filtered_time, filtered_record = filter.filter_with_time(tag, filtered_time, filtered_record)
                    throw :break_loop unless filtered_record && filtered_time
                  rescue => e
                    filter.router.emit_error_event(tag, filtered_time, filtered_record, e)
                  end
                else
                  begin
                    filtered_record = filter.filter(tag, filtered_time, filtered_record)
                    throw :break_loop unless filtered_record
                  rescue => e
                    filter.router.emit_error_event(tag, filtered_time, filtered_record, e)
                  end
                end
              end

              new_es.add(filtered_time, filtered_record)
            end
          end
          new_es
        end

        def optimizable?
          return @optimizable unless @optimizable.nil?
          fs_filters = filters_having_filter_stream
          @optimizable = if fs_filters.empty?
                           true
                         else
                           # skip log message when filter is only 1, because its performance is same as non optimized chain.
                           if @filters.size > 1 && fs_filters.size >= 1
                             $log.info "disable filter chain optimization because #{fs_filters.map(&:class)} uses `#filter_stream` method."
                           end
                           false
                         end
        end

        def filters_having_filter_stream
          @filters_having_filter_stream ||= @filters.select do |filter|
            filter.class.instance_methods(false).include?(:filter_stream)
          end
        end

        def reset_optimization
          @optimizable = nil
          @filters_having_filter_stream = nil
        end
      end
    end

    def find(tag)
      pipeline = nil
      @match_rules.each_with_index { |rule, i|
        if rule.match?(tag)
          if rule.collector.is_a?(Plugin::Filter)
            pipeline ||= Pipeline.new
            pipeline.add_filter(rule.collector)
          else
            if pipeline
              pipeline.set_output(rule.collector)
            else
              # Use Output directly when filter is not matched
              pipeline = rule.collector
            end
            return pipeline
          end
        end
      }

      if pipeline
        # filter is matched but no match
        pipeline.set_output(@default_collector)
        pipeline
      else
        nil
      end
    end
  end
end
