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

    class Agent < Fluentd::Agent
      def initialize
        super
      end

      config_param :log_level, :string, :default => nil

      def configure(conf)
        super

        if conf['log_level']
          @log = logger.dup # logger is initialized at configurable.rb
          begin
            @log.level = conf['log_level'] # reuse error handling of Fluentd::Logger
          rescue ArgumentError => e
            raise ConfigError, "log_level should be 'fatal', 'error', 'warn', 'info', or 'debug'"
          end
        end
      end
    end
  end
end

