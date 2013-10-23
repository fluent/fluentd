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

  class Engine
    module ClassMethods
      # Rinda::TupleSpace initialized at Server#before_run
      attr_accessor :tuples

      # Fluentd::Logger instance
      attr_accessor :logger
      alias_method :log, :logger

      # PluginRegistry instance
      attr_accessor :plugins

      # SocketManager instance
      attr_accessor :sockets

      # RootAgent instance
      attr_accessor :root_agent

      # Globally shared data initialized by Server#initialize
      attr_accessor :shared_data

      def now
        Time.now
      end
    end

    extend ClassMethods
  end

end
