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

require 'fluent/time'

module Fluent
  module PluginHelper
    module EventEmitter
      # stop     : [-]
      # shutdown : disable @router
      # close    : [-]
      # terminate: [-]

      def router
        @_event_emitter_used_actually = true
        if @_event_emitter_lazy_init
          @router = @primary_instance.router
        end
        @router
      end

      def router=(r)
        # not recommended now...
        @router = r
      end

      def has_router?
        true
      end

      def event_emitter_used_actually?
        @_event_emitter_used_actually
      end

      def event_emitter_router(label_name)
        if label_name
          Engine.root_agent.find_label(label_name).event_router
        elsif self.respond_to?(:as_secondary) && self.as_secondary
          if @primary_instance.has_router?
            @_event_emitter_lazy_init = true
            nil # primary plugin's event router is not initialized yet, here.
          else
            @primary_instance.context_router
          end
        else
          # `Engine.root_agent.event_router` is for testing
          self.context_router || Engine.root_agent.event_router
        end
      end

      def initialize
        super
        @_event_emitter_used_actually = false
        @_event_emitter_lazy_init = false
        @router = nil
      end

      def configure(conf)
        require 'fluent/engine'
        super
        @router = event_emitter_router(conf['@label'])
      end

      def after_shutdown
        @router = nil
        super
      end

      def close # unset router many times to reduce test cost
        @router = nil
        super
      end

      def terminate
        @router = nil
        super
      end
    end
  end
end
