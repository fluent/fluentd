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

require 'fluent/test/driver/base'

require 'fluent/plugin/base'
require 'fluent/plugin_id'
require 'fluent/log'
require 'fluent/plugin_helper'

module Fluent
  module Test
    module Driver
      class OwnerDummy < Fluent::Plugin::Base
        include PluginId
        include PluginLoggerMixin
        include PluginHelper::Mixin
      end

      class BaseOwned < Base
        attr_accessor :section_name

        def initialize(klass, opts: {}, &block)
          super

          owner = OwnerDummy.new
          if opts
            owner.system_config_override(opts)
          end
          owner.log = TestLogger.new

          if @instance.respond_to?(:owner=)
            @instance.owner = owner
            if opts
              @instance.system_config_override(opts)
            end
          end

          @logs = owner.log.out.logs
          @section_name = ''
        end

        def configure(conf, syntax: :v1)
          if conf.is_a?(Fluent::Config::Element)
            @config = conf
          elsif conf.is_a?(Hash)
            @config = Fluent::Config::Element.new(@section_name, "", Hash[conf.map{|k,v| [k.to_s, v]}], [])
          else
            @config = Fluent::Config.parse(conf, @section_name, "", syntax: syntax)
          end
          @instance.configure(@config)
          self
        end
      end
    end
  end
end
