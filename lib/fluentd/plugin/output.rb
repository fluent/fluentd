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

    require_relative '../agent'
    require_relative '../collector'
    require_relative '../actor'
    require_relative '../worker_global_methods'

    class Output < Agent
      include Collector

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
        Fluentd.log.warn "emit error", :error => e
        stats.emit_error(tag, time, record)
        nil
      end
    end

  end
end
