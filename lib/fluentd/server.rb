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

  class Server
    def initialize(conf=nil, &block)
      if conf
        if conf.is_a?(Config)
          block = Proc.new { conf }
        else
          block = Proc.new { Config.parse(conf, '(data)') }
        end
      end
      @config_load_proc = block

      # initial logger
      @log = Logger.new(STDERR)
    end

    def setup!
      return if @setup

      @engine = Engine.new(reload_config)
      @engine.setup!

      @setup = true
    end

    def run
      @log.info "Fluentd #{VERSION}"
      setup!

      install_signal_handlers

      @engine.run

      return nil
    rescue
      @log.error "#{$!.class}: #{$!}"
      @log.warn_backtrace
      return nil
    end

    def stop(immediate)
      @log.info immediate ? "Received immediate stop" : "Received graceful stop"
      begin
        @engine.stop(immediate) if @engine
      rescue
        @log.error "failed to stop: #{$!}"
        @log.warn_backtrace
        return false
      end
      return true
    end

    def restart(immediate)
      @log.info immediate ? "Received immediate restart" : "Received graceful restart"
      begin
        @engine.restart(immediate, reload_config)
      rescue
        @log.error "failed to restart: #{$!}"
        @log.warn_backtrace
        return false
      end
      return true
    end

    def logrotate
      @log.info "reopen log file"
      @engine.logrotate
      @log.reopen!
      return true
    end

    private
    def reload_config
      # TODO error handling
      conf = @config_load_proc.call

      # setup logger
      old_log = @log
      log = Logger.new(conf['log'] || STDERR)
      old_log.close if old_log

      log.hook_io(STDOUT)
      log.hook_io(STDERR)

      define_logger(conf, log)
      @log = log

      return conf
    end

    def define_logger(conf, log)
      conf.define_singleton_method(:logger) { log }
      conf.elements.each {|e|
        define_logger(e, log)
      }
    end

    def install_signal_handlers(&block)
      SignalQueue.start do |sig|
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
          logrotate
        end
      end
    end
  end

end
