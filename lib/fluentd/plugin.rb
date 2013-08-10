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

  require_relative 'worker_global_methods'
  require_relative 'configurable'

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

      # delegates methods to Fluentd.plugin defined in process_global_methods.rb

      def_delegators 'Fluentd.plugin',
        :register_input, :register_output, :register_filter, :register_buffer,
        :new_input, :new_output, :new_filter, :new_buffer

      # Configurable.register_type is global setting
      def_delegators 'Fluentd::Configurable', :register_type
    end

    extend ClassMethods
  end

  # loads base classes of plugins
  require_relative 'event_collection'
  require_relative 'config_error'
  require_relative 'plugin/output'
  require_relative 'plugin/input'
  require_relative 'plugin/filter'
  require_relative 'plugin/buffer'
  require_relative 'plugin/buffered_output'
  require_relative 'plugin/object_buffered_output'

end
