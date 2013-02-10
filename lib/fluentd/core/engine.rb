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
    #include Configurable

    def initialize(conf)
      @conf = conf
      @log = conf.log
      @finish_flag = BlockingFlag.new

      @pm = ProcessManager.new(conf)

      @message_bus = nil
      @message_bus_proxy = Collectors::ProxyCollector.new(nil)
    end

    # backward compatibility
    def self.setup_compat!(bus, plugin)
      require 'fluent/load'
      Fluent::EngineClass.setup!(bus, plugin)
    end

    def configure(conf)
      @conf = conf
      @log = conf.log
    end

    def run
      setup!

      begin
        @pm.run

      rescue
        @log.error $!
        @log.warn_backtrace

      ensure
        @pm.stop(false)
        @pm.join
      end
    end

    def setup!
      return if @setup

      @plugin = PluginClass.load!(@conf)
      @conf.context.plugin = @plugin

      Engine.setup_compat!(@message_bus_proxy, @plugin)

      configure(@conf)

      new_bus = LabeledMessageBus.new
      new_bus.configure(@conf)

      @pm.setup!(new_bus, @conf)

      @message_bus = new_bus
      @message_bus_proxy.reset(@message_bus)

      @setup = true
    end

    def restart(immediate, conf)
      return if @finish_flag.set?

      configure(conf)

      new_bus = LabeledMessageBus.new
      new_bus.configure(conf)

      @pm.restart(immediate, new_bus, conf)

      @message_bus = new_bus
      @message_bus_proxy.reset(@message_bus)
    end

    def stop(immediate)
      @pm.stop(immediate)
      @finish_flag.set!
      self
    end

    def logrotate
      @pm.logrotate
    end
  end

end
