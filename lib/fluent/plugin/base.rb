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
require 'fluent/system_config'

module Fluent
  module Plugin
    class Base
      include Configurable
      include SystemConfig::Mixin

      State = Struct.new(:configure, :start, :after_start, :stop, :before_shutdown, :shutdown, :after_shutdown, :close, :terminate)

      attr_accessor :under_plugin_development

      def initialize
        super
        @_state = State.new(false, false, false, false, false, false, false, false, false)
        @_context_router = nil
        @under_plugin_development = false
      end

      def has_router?
        false
      end

      def configure(conf)
        super
        @_state ||= State.new(false, false, false, false, false, false, false, false, false)
        @_state.configure = true
        self
      end

      def string_safe_encoding(str)
        unless str.valid_encoding?
          log.info "invalid byte sequence is replaced in `#{str}`" if self.respond_to?(:log)
          str = str.scrub('?')
        end
        yield str
      end

      def context_router=(router)
        @_context_router = router
      end

      def context_router
        @_context_router
      end

      def start
        @_state.start = true
        self
      end

      def after_start
        @_state.after_start = true
        self
      end

      def stop
        @_state.stop = true
        self
      end

      def before_shutdown
        @_state.before_shutdown = true
        self
      end

      def shutdown
        @_state.shutdown = true
        self
      end

      def after_shutdown
        @_state.after_shutdown = true
        self
      end

      def close
        @_state.close = true
        self
      end

      def terminate
        @_state.terminate = true
        self
      end

      def configured?
        @_state.configure
      end

      def started?
        @_state.start
      end

      def after_started?
        @_state.after_start
      end

      def stopped?
        @_state.stop
      end

      def before_shutdown?
        @_state.before_shutdown
      end

      def shutdown?
        @_state.shutdown
      end

      def after_shutdown?
        @_state.after_shutdown
      end

      def closed?
        @_state.close
      end

      def terminated?
        @_state.terminate
      end
    end
  end
end
