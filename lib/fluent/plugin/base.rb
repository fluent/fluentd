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

require 'fluent/plugin'
require 'fluent/configurable'
require 'fluent/plugin_id'
require 'fluent/log'
require 'fluent/plugin_helper'

module Fluent
  module Plugin
    class Base
      include Configurable
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin

      State = Struct.new(:configure, :start, :stop, :shutdown, :close, :terminate)

      def initialize
        super
        @state = State.new(false, false, false, false, false, false)
      end

      def has_router?
        false
      end

      def configure(conf)
        super
        @state.configure = true
        self
      end

      def start
        @log.reset
        @state.start = true
        self
      end

      def stop
        @state.stop = true
        self
      end

      def shutdown
        @state.shutdown = true
        self
      end

      def close
        @state.close = true
        self
      end

      def terminate
        @state.terminate = true
        @log.reset
        self
      end

      def configured?
        @state.configure
      end

      def started?
        @state.start
      end

      def stopped?
        @state.stop
      end

      def shutdown?
        @state.shutdown
      end

      def closed?
        @state.close
      end

      def terminated?
        @state.terminate
      end
    end
  end
end
