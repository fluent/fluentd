#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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
    def initialize(config)
      @config = config
      @finish_flag = BlockingFlag.new

      @pm = ProcessManager.new(config)

      @message_bus = nil
      @message_bus_proxy = Collectors::ProxyCollector.new(nil)
    end

    def self.setup!
      # TODO re-design this method
      return if Fluentd.const_defined?(:Plugin)

      plugin = Fluentd.const_set(:Plugin, PluginClass.new)

      # backward compatibility
      require 'fluent/load'
      Fluent::EngineClass.setup!(@message_bus_proxy, plugin)

      plugin.load_plugins
    end

    def run
      Engine.setup!

      restart(nil, @config)

      begin
        @pm.run
      ensure
        @pm.stop(false)
        @pm.join
      end
    end

    def restart(immediate, config)
      return if @finish_flag.set?

      @config = config

      new_bus = LabeledMessageBus.new
      new_bus.configure(config)

      @pm.restart(immediate, new_bus, config)

      @message_bus = new_bus

      @message_bus_proxy.reset(@message_bus)
    end

    def stop(immediate)
      @pm.stop(immediate)
      @finish_flag.set!
      self
    end

    def logrotate
      #@pm.logrotate
    end
  end

end
