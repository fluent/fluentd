#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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


  class Agent
    include Configurable

    def initialize
      @process_groups = []
    end

    attr_reader :process_groups

    # TODO multi_process?

    def configure(conf)
      @log = conf.log

      # TODO process_group
      if gr = conf['process_group']
        @process_groups = [gr]
      end

      super
    end

    def start
    end

    def stop
    end

    def shutdown
    end
  end


end

