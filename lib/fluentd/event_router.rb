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

  require_relative 'collector'

  #
  # EventRouter matches a tag of an event with registered patterns and route the event to
  # the matched collector. If no patterns match, EventRouter routes the event to
  # #default_collector.
  #
  # Input plugins start matching from the beginning of the matching rules. On the other hand,
  # filter plugins start matching from the middle:
  #
  #  <source> matches from offset=0:
  #    [
  #      0:match  a.**
  #      1:filter b.**      --> match
  #      2.match  b.**            |
  #    ] --- match_patterns       |
  #                               |
  #    +--------------------------+
  #    |
  #  <filter b.**> matches from offset=2:
  #    [
  #      0:match  a.**      --- skip
  #      1:filter b.**      --- skip
  #      2.match  b.**      --> match
  #    ] --- match_patterns
  #
  # See also EventEmitter#configure_agent and #configure_agent_with_offset.
  #
  class EventRouter
    include Collector

    def initialize(default_collector)
      @default_collector = default_collector
      @match_caches = []
      @match_patterns = []
      @match_collectors = []
    end

    attr_accessor :default_collector

    MATCH_CACHE_SIZE = 1024

    def add_collector(pattern, collector)
      @match_patterns << pattern
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

    def match_offset(offset, tag)
      cache = (@match_caches[offset] ||= MatchCache.new)
      collector = cache[tag]

      unless collector
        collector = find(tag, offset) || @default_collector
        collector = collector.short_circuit(tag)
        cache[tag] = collector
      end

      return collector
    end

    def short_circuit(tag)
      match_offset(0, tag).short_circuit(tag)
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
  end

end
