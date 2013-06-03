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
  # MessageRouter matches a tag of an event with registered patterns and route the event to
  # the matched collector (See #emits method).
  # If no patterns match, MessageRouter routes the event to #default_collector.
  #
  # MessageRouter forms a tree structure where the root node is a RootAgent:
  #
  # MessageRouter              MessageRouter
  #  #default_collector --+     #default_collector --+
  #                       |                          |
  #                       +--> MessageRouter         |
  # MessageRouter         |     #default_collector --+--> RootAgent
  #  #default_collector --+                          |     #default_collector --> NoMatchCollector
  #                            MessageRouter         |
  #                             #default_collector --+
  #

  class MessageRouter
    include Collector

    def initialize(default_collector)
      @default_collector = default_collector
      @match_caches = []
      @match_patterns = []
      @match_collectors = nil
    end

    attr_accessor :default_collector

    MATCH_CACHE_SIZE = 1024

    def add_collector(pattern, collector)
      @match_patterns << pattern
      @match_collectors ||= []
      @match_collectors << collector
      nil
    end

    def current_offset
      Offset.new(self, @match_patterns.size+1)
    end

    def emit(tag, time, record)
      match_offset(0, tag).emit(tag, time, record)
    end

    def emits(tag, es)
      match_offset(0, tag).emits(tag, es)
    end

    def emit_offset(offset, tag, time, record)
      match_offset(tag, offset).emit(tag, time, record)
    end

    def emits_offset(offset, tag, es)
      match_offset(offset, tag).emits(tag, es)
    end

    def match(tag)
      match_offset(0, tag)
    end

    def match_offset(offset, tag)
      unless @match_collectors
        return @default_collector_short_circuit ||= @default_collector.short_circuit
      end

      cache = (@match_caches[offset] ||= MatchCache.new)
      collector = cache[tag]

      unless collector
        collector = find(tag, offset)
        if collector
          collector = collector.short_circuit(tag)
        else
          collector = (@default_collector_short_circuit ||= @default_collector.short_circuit)
        end
        cache[tag] = collector
      end

      return collector
    end

    def short_circuit(tag)
      match(tag).short_circuit(tag)
    end

    def short_circuit_offset(offset, tag)
      match_offset(offset, tag).short_circuit(tag)
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

      def short_circuit(tag)
        @router.short_circuit_offset(@offset, tag)
      end
    end

    private

    def find(tag, offset)
      @match_patterns.each_with_index {|pat,i|
        next if i < offset
        if pat.match?(tag)
          return @match_collectors[i]
        end
      }
      return nil
    end

    class MatchCache < Hash
      def initialize
        super
        @keys = []
      end

      def []=(key, value)
        if @keys.size >= MATCH_CACHE_SIZE
          delete @keys.shift
        end
        super(key, value)
      end
    end
  end

end
