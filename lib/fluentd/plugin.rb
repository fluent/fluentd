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
module Fluentd

  require 'fluentd/engine'

  #
  # Plugin provides Plugin.register_XXX where XXX is one of:
  #
  #   * input (input plugins)
  #   * output (output plugins)
  #   * buffer (buffer plugins)
  #   * type (configuration types)
  #
  module Plugin

    # delegates methods to Engine.plugins, which is an instance of
    # PluginRegistry.
    module ClassMethods
      extend Forwardable

      def_delegators 'Fluentd::Engine.plugins',
        :register_input, :register_output, :register_buffer,
        :new_input, :new_output, :new_buffer,
        :register_type, :lookup_type
    end

    extend ClassMethods
  end

end
