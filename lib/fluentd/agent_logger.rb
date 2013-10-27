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

  require 'fluentd/logger'
  require 'forwardable'

  #
  # AgentLogger is a wrapper of Logger.
  #
  # AgentLogger has its own log level separated from logger. Thus
  # users can set different log level for each agent instance.
  #
  # Another functionality is to forward logs to RootAgent#emit_log.
  #
  class AgentLogger < Logger
    def initialize(logger)
      @logger = logger
      @root_agent = nil
      self.level = @logger.level

      # don't have to call super here because
      # << is delegated to the parent @logger.
    end

    # delegate global options to the parent @logger
    extend Forwardable
    def_delegators '@logger',
      :format_event,
      :time_format, :time_format=,
      :enable_filename, :enable_filename=,
      :enable_color, :enable_color=

    attr_writer :root_agent

    def add_event(level, time, message, record, caller_stack)
      super  # calls #<<
      if @root_agent
        record['message'] = message
        @root_agent.log_collector.emit("fluentd", time.to_i, record)
      end
      nil
    end

    # override
    def <<(message)
      @logger << message
    end
  end

end
