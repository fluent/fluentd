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

  require 'fluentd/engine'
  require 'fluentd/configurable'

  #
  # Plugin provides Plugin.register_XXX where XXX is one of:
  #
  #   * input (input plugins)
  #   * output (output plugins)
  #   * filter (filter plugins)
  #   * buffer (buffer plugins)
  #   * type (configuration types)
  #
  module Plugin

    module ClassMethods
      extend Forwardable

      # delegates methods to Engine.plugins
      def_delegators 'Fluentd::Engine.plugins',
        :register_input, :register_output, :register_filter, :register_buffer,
        :new_input, :new_output, :new_filter, :new_buffer

      # Configurable.register_type is global setting
      def_delegators 'Fluentd::Configurable', :register_type
    end

    extend ClassMethods
  end

  # loads base classes of plugins
  require 'fluentd/event_collection'
  require 'fluentd/config_error'
  require 'fluentd/plugin/output'
  require 'fluentd/plugin/input'
  require 'fluentd/plugin/filter'
  require 'fluentd/plugin/buffer'
  require 'fluentd/plugin/buffered_output'
  require 'fluentd/plugin/object_buffered_output'
  require 'fluentd/plugin/time_sliced_output'
end
