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

  require 'serverengine'
  require 'drb'

  require_relative 'engine'
  require_relative 'worker'
  require_relative 'socket_manager'
  require_relative 'server'

  module WorkerLauncher
    def initialize
      @pm = ServerEngine::ProcessManager.new

      @stop_flag = ServerEngine::BlockingFlag.new

      # ruby command/fluentd-worker $(drb_uri)
      @cmdline = [
        RbConfig.ruby,
        File.expand_path(File.join(File.dirname(__FILE__), 'command', 'fluentd-worker.rb')),
        worker_id.to_s,
        server.drb_uri,
      ]

      super
    end

    # TODO
    ## ServerEngine callback
    #def before_fork
    #  # first attempt should not restart worker automatically to
    #  # notice users errors.
    #  run_worker
    #
    #  # check worker became ready to run at least once
    #  unless Engine.shared_data["fluentd_worker_#{worker_id}_configured"]
    #    raise "Worker configuration failed"
    #  end
    #end

    # ServerEngine callback
    def run
      first = true
      until @stop_flag.set?
        unless first
          logger.info "Worker #{worker_id} exited unexpectedly. Restarting."
          first = false
        end
        run_worker
      end
    rescue
      logger.error e.to_s
      logger.error_backtrace e.backtrace
      raise
    ensure
      @pm.close
    end

    def run_worker
      logger.info "Starting worker #{worker_id}"

      monitor = @pm.spawn(*@cmdline)
      until monitor.try_join
        if @stop_flag.wait_for_set(1)
          monitor.start_graceful_stop!
          monitor.join
          return
        end
      end
    end

    # ServerEngine callback
    def stop
      @stop_flag.set!
    end

    # called by command/fluentd-worker.rb
    def self.main
      # @cmdline includes worker_id and DRb URI
      worker_id = ARGV[0].to_i
      drb_uri = ARGV[1]

      # Get config from the parent process
      # See also Server#before_run
      parent_engine = DRb::DRbObject.new_with_uri(drb_uri)

      Engine.shared_data = parent_engine.shared_data

      options = Engine.shared_data['fluentd_options']

      options[:load_path].each {|path|
        $LOAD_PATH << path
      }

      # TODO
      Engine.sockets = SocketManager::NonManagedAPI.new

      # initialize worker instance
      worker = Worker.new(options)
      install_signal_handlers(worker)

      # read config file again for each worker because
      # Config::DSL::Parser allows users to set Proc object
      # which can't be exchanged across processes.
      conf = Server.read_config(options[:config_path])

      # get <worker> element for this worker process
      worker_elements = conf.elements.select {|e| e.name == 'worker' }
      worker_element = worker_elements[worker_id]
      worker_element['id'] ||= worker_id

      # set process name
      # :worker_process_name is set by command/fluentd.rb
      $0 = "#{options[:worker_process_name]} #{worker_element['id']}"

      # change user and group
      chuser = worker_element['chuser'] || options[:chuser]
      chgroup = worker_element['chgroup'] || options[:chgroup]
      ServerEngine::Daemon.change_privilege(chuser, chgroup)

      # configure worker using the config file
      worker.configure(worker_element)

      # this worker is ready to run
      Engine.shared_data["fluentd_worker_#{worker_id}_configured"] = true

      worker.run
    end

    def self.install_signal_handlers(worker)
      ServerEngine::SignalThread.new do |st|
        st.trap(ServerEngine::Daemon::Signals::GRACEFUL_STOP) { worker.stop }
        st.trap(ServerEngine::Daemon::Signals::IMMEDIATE_STOP, 'SIG_DFL')
        st.trap(ServerEngine::Daemon::Signals::GRACEFUL_RESTART) { worker.stop }
        st.trap(ServerEngine::Daemon::Signals::IMMEDIATE_RESTART, 'SIG_DFL')
        st.trap(ServerEngine::Daemon::Signals::RELOAD) { Engine.logger.reopen! }
        st.trap(ServerEngine::Daemon::Signals::DETACH) { worker.stop }
        st.trap(ServerEngine::Daemon::Signals::DUMP) { Sigdump.dump }
      end
    end
  end
end

