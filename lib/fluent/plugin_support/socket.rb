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

module Fluent
  module PluginSupport
    module Socket
      SockInfo = Struct.new(:proto, :bind, :port)

      attr_reader :_listener_states

      def initialize
        super
        @_listeners = []
        @_listener_states = {} # sock => running[bool]
      end

      def socket_listener_add(proto, bind, port)
        sinfo = SockInfo.new(proto, bind, port)
        @_listeners << sinfo
        @_listener_states[sinfo] = false
      end

      def socket_listener_listen(proto, bind, port)
        sinfo = @_listeners.find{|s| s.proto == proto && s.bind == bind && s.port == port }
        raise "BUG: socket_listener_listen called without socket_listener_add, #{proto},#{bind},#{port}" unless sinfo
        @_listener_states[sinfo] = true
      end

      def terminate
        super
        @_listeners = []
        @_listener_states = {}
      end
    end
  end
end
