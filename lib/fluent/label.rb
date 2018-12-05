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

require 'fluent/agent'

module Fluent
  class Label < Agent
    def initialize(name, log:)
      super(log: log)

      @context = name
      @root_agent = nil
    end

    attr_accessor :root_agent

    def configure(conf)
      super

      if conf.elements('match').size == 0
        raise ConfigError, "Missing <match> sections in <label #{@context}> section"
      end
    end

    def emit_error_event(tag, time, record, e)
      @root_agent.emit_error_event(tag, time, record, e)
    end

    def handle_emits_error(tag, es, e)
      @root_agent.handle_emits_error(tag, es, e)
    end
  end
end
