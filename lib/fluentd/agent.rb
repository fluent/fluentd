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

  require_relative 'configurable'

  #
  # Agent is the base class of input, output and filter plugins.
  # See root_agent.rb for details.
  #
  class Agent
    include Configurable

    def initialize
      @agents = []
      init_configurable  # initialize Configurable
      super
    end

    # nested agents
    attr_reader :agents

    attr_reader :parent_agent

    attr_reader :root_agent

    def parent_agent=(parent)
      @root_agent = parent.root_agent
      parent._add_agent(self)
      @parent_agent = parent
    end

    def configure(conf)
      super
    end

    def start
    end

    def stop
    end

    def shutdown
    end

    def _add_agent(agent)
      @agents << agent
      self
    end
  end

end
