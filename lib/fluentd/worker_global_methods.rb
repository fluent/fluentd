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

  require_relative 'logger'
  require_relative 'plugin'
  require_relative 'socket_manager'

  # These variables are initialized by
  # WorkerLauncher#configuration
  module ClassMethods
    attr_accessor :plugin
    attr_accessor :logger
    attr_accessor :socket_manager

    alias_method :log, :logger

    def setup!(opts={})
      Fluentd.logger = opts[:logger] || Fluentd::Logger.new(STDOUT)
      Fluentd.plugin = opts[:plugin] || PluginRegistry.new
      Fluentd.socket_manager = opts[:socket_manager] || SocketManager::NonManagedAPI
    end
  end

  extend ClassMethods

end
