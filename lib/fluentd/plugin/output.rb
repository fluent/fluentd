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
  module Plugin

    require 'fluentd/agent'
    require 'fluentd/collector'
    require 'fluentd/actor'
    require 'fluentd/engine'

    #
    # Output is an Agent implementing Collector interface.
    # You need to implement #emit(tag, time, record) at least to satisfy
    # the requirements of Collector interface.
    #
    class Output < Agent
      include Collector

      # provides #actor
      include Actor::AgentMixin

      # must be implemented in the extending class
      def emit(tag, time, record)
        raise NoMethodError, "#{self.class}#emit(tag, time, record) is not implemented"
      end

      def emits(tag, es)
        es.each {|time,record|
          handle_error(tag, time, record) {
            emit(tag, time, record)
          }
        }
        nil
      end

      def handle_error(tag, time, record, &block)
        block.call
      rescue => e
        log.warn "emit error", :error => e
        @root_agent.emit_error(tag, time, record)
        nil
      end
    end

  end
end
