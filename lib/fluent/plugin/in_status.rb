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

require 'fluent/status'
require 'fluent/plugin_support/timer'

module Fluent::Plugin
  class StatusInput < Fluent::Plugin::Input
    include Fluent::PluginSupport::Timer

    Fluent::Plugin.register_input('status', self)

    config_param :emit_interval, :time, default: 60
    config_param :tag, :string

    def start
      super

      timer_execute(interval: @emit_interval, repeat: true) do
        now = Fluent::Engine.now
        Fluent::Status.each do |record|
          router.emit(@tag, now, record)
        end
      end
    end
  end
end
