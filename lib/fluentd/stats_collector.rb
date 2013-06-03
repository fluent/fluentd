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

  class StatsCollector
    def initialize(root_agent)
      @root_agent = root_agent
    end

    def emit_error(tag, time, record)
      emits_error(tag, OneEventStream.new(time, record))
    end

    def emits_error(tag, es)
      @root_agent.emits_label(RootAgent::ERROR_LABEL, tag, es)
    rescue
      # TODO log
    end

    def emit_log(tag, time, record)
      emits_log(tag, OneEventStream.new(time, record))
    end

    def emits_log(tag, es)
      @root_agent.emits_label(RootAgent::LOG_LABEL, tag, es)
    rescue
      # TODO log
    end
  end

  class NullStatsCollector
    def emit_error(tag, time, record)
    end

    def emits_error(tag, es)
    end

    def emit_log(tag, time, record)
    end

    def emits_log(tag, es)
    end
  end

end
