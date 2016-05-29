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

require 'fluent/configurable'

module Fluent
  module Compat
    module PropagateDefault
      # This mixin is to prepend to 3rd party plugins of v0.12 APIs.
      # 3rd party plugins may override default values of some parameters, like `buffer_type`.
      # But default values of such parameters will NOT used, but defaults of <buffer>@type</buffer>
      # will be used in fact. It should bring troubles.
      # This mixin defines Class method .config_param and .config_set_default (which should be used by extend)
      # to propagate changes of default values to subsections.
      def self.included(mod)
        mod.extend(ClassMethods)
      end

      module ClassMethods
        CONFIGURABLE_CLASS_METHODS = Fluent::Configurable::ClassMethods

        def config_param(name, type = nil, **kwargs, &block)
          CONFIGURABLE_CLASS_METHODS.instance_method(:config_param).bind(self).call(name, type, **kwargs, &block)
          pparams = propagate_default_params
          if kwargs.has_key?(:default) && pparams[name.to_s]
            newer = pparams[name.to_s].to_sym
            overridden_default_value = kwargs[:default]

            CONFIGURABLE_CLASS_METHODS.instance_method(:config_section).bind(self).call(:buffer) do
              config_set_default newer, overridden_default_value
            end
          end
        end

        def config_set_default(name, defval)
          CONFIGURABLE_CLASS_METHODS.instance_method(:config_set_default).bind(self).call(name, defval)
          pparams = propagate_default_params
          if pparams[name.to_s]
            newer = pparams[name.to_s].to_sym

            CONFIGURABLE_CLASS_METHODS.instance_method(:config_section).bind(self).call(:buffer) do
              self.config_set_default newer, defval
            end
          end
        end
      end
    end
  end
end
