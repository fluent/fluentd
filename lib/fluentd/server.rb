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
          block = Proc.new { Config.evaluate(conf) }
        end
      end
      @config_load_proc = block

      # initial logger
      STDERR.sync = true
      @log = DaemonsLogger.new(STDERR)
    end

    def run
      @log.info "Fluentd #{VERSION}"

      install_signal_handlers

      @engine = Engine.new(load_config)

      #begin
        @engine.run
      #ensure
      #  @engine.shutdown(true)
      #end

      return nil
    rescue
      @log.error "#{$!.class}: #{$!}"
      $!.backtrace.each {|x| @log.warn "\t#{x}" }
      return nil
    end

    def stop(immediate)
      @log.info immediate ? "Received immediate stop" : "Received graceful stop"
      begin
        @engine.stop(immediate) if @engine
      rescue
        @log.error "failed to stop: #{$!}"
        $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
        return false
      end
      return true
    end

    def restart(immediate)
      @log.info immediate ? "Received immediate restart" : "Received graceful restart"
      begin
        @engine.restart(immediate, load_config)
      rescue
        @log.error "failed to restart: #{$!}"
        $!.backtrace.each {|bt| @log.warn "\t#{bt}" }
        return false
      end
      return true
    end

    def logrotate
      @log.info "reopen a log file"
      @engine.logrotate
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
