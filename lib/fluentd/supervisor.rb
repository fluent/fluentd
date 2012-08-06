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


  #def self.run(conf=nil, &block)
  #  Supervisor.run(conf, &block)
  #end

  class Supervisor
    def self.run(conf=nil, &block)
      new(conf, &block).run
    end

    def initialize(conf=nil, &block)
      if conf
        if conf.is_a?(Config)
          block = Proc.new { conf }
        else
          block = Proc.new { Config.evaluate(conf) }
        end
      end
      @config_load_proc = block

      # initial logger
      STDERR.sync = true
      @log = DaemonsLogger.new(STDERR)

      # load plugins
      Plugin.load_plugins

      @pm = ProcessManager.new
      @message_bus = nil
    end

    attr_reader :message_bus

    def run
      @log.info "Fluentd #{VERSION}"

      install_signal_handlers do
        restart(:_initial_start)
        begin
          @pm.run
        ensure
          begin
            #@pm.stop(true)  # TODO
            @pm.join
          ensure
            @pm.shutdown
          end
        end
      end

      return nil
    rescue
      @log.error "#{$!.class}: #{$!}"
      $!.backtrace.each {|x| @log.warn "\t#{x}" }
      return nil
    end

    def restart(immediate)
      conf = load_config

      new_bus = LabeledMessageBus.new
      begin
        new_bus.configure(conf)

        if immediate != :_initial_start
          @pm.stop(immediate)
          @pm.join
        end
        @pm.reset(new_bus, conf)

        @message_bus = new_bus
        new_bus = nil

      ensure
        new_bus.shutdown if new_bus
      end
    end

    def stop(immediate)
      @pm.stop(immediate)
    end

    def logrotated
      #@log.info "reopen a log file"
      #@engine.logrotated
      @log.reopen!
      return true
    end

    private
    def load_config
      # TODO error handling
      conf = @config_load_proc.call

      old_log = @log
      log = DaemonsLogger.new(conf['log'] || STDERR)
      old_log.close if old_log

      conf['logger'] = log
      @log = log

      return conf
    end

    def install_signal_handlers(&block)
      sig = SignalQueue.start do |sig|
        sig.trap :TERM do
          stop(false)
        end
        sig.trap :INT do
          stop(false)
        end

        sig.trap :QUIT do
          stop(true)
        end

        sig.trap :USR1 do
          restart(true)
        end

        sig.trap :HUP do
          restart(false)
        end

        sig.trap :USR2 do
          logrotated
        end
      end

      begin
        block.call
      ensure
        sig.shutdown
      end
    end
  end


end
