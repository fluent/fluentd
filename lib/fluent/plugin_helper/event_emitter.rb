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

        return Engine.root_agent.source_only_router if @_event_emitter_force_source_only_router

        if @_event_emitter_lazy_init
          @router = @primary_instance.router
        end
        if @router.respond_to?(:caller_plugin_id=)
          @router.caller_plugin_id = self.plugin_id
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

      def event_emitter_apply_source_only
        @_event_emitter_force_source_only_router = true
      end

      def event_emitter_cancel_source_only
        @_event_emitter_force_source_only_router = false
      end

      def event_emitter_router(label_name)
        if label_name
          owner = event_emitter_owner_agent
          current = Engine.root_agent
          if owner.nil? || owner.equal?(current) || owner.shutdown_started?
            LabelRouter.find(current, label_name)
          else
            # During a graceful reload, this plugin belongs to the old
            # configuration while Engine.root_agent is the new one.
            # See LabelRouter.
            LabelRouter.new(owner, label_name).tap(&:current)
          end
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
        @_event_emitter_force_source_only_router = false
        @_event_emitter_owner_agent = nil
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

      private

      # The RootAgent of the configuration this plugin belongs to, or nil if
      # it is not known (e.g. a plugin created without an agent in tests).
      # The agents answer this themselves (Label#root_agent, RootAgent#root_agent).
      # It does not change after configure, so it is cached once known.
      def event_emitter_owner_agent
        return @_event_emitter_owner_agent if @_event_emitter_owner_agent
        router = self.context_router
        handler = router.respond_to?(:emit_error_handler) ? router.emit_error_handler : nil
        @_event_emitter_owner_agent = handler.respond_to?(:root_agent) ? handler.root_agent : nil
      end

      # The router that event_emitter_router(label_name) returns to a plugin
      # of the old configuration during a graceful reload. Outside a reload,
      # event_emitter_router returns the label's EventRouter itself.
      #
      # Engine.root_agent becomes the new configuration before its <match>
      # sections are added, so a label looked up there can drop the event and
      # leave the tag cached as unmatched (#5594). While the old configuration
      # is still running, events go to its own label instead, like @label and
      # relabel.
      #
      # Once the old configuration has started to shut down, its labels may
      # already have dropped their routers, so events go to the label of
      # Engine.root_agent, as before. This is decided on each emit, because a
      # plugin may take the router before the old configuration starts to shut
      # down and emit after that.
      class LabelRouter
        def self.find(agent, label_name)
          if label_name == "@ROOT"
            agent.event_router
          else
            agent.find_label(label_name).event_router
          end
        end

        attr_reader :label_name

        def initialize(owner_agent, label_name)
          @owner_agent = owner_agent
          @label_name = label_name
        end

        def current
          owner = @owner_agent
          agent = (owner && !owner.shutdown_started?) ? owner : Engine.root_agent
          LabelRouter.find(agent, @label_name)
        end

        def emit(tag, time, record)
          current.emit(tag, time, record)
        end

        def emit_array(tag, array)
          current.emit_array(tag, array)
        end

        def emit_stream(tag, es)
          current.emit_stream(tag, es)
        end

        def respond_to_missing?(name, include_private = false)
          current.respond_to?(name, include_private) || super
        end

        def method_missing(name, *args, &block)
          router = current
          if router.respond_to?(name)
            router.__send__(name, *args, &block)
          else
            super
          end
        end
      end
    end
  end
end
