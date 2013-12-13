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
module Fluentd

  require 'serverengine'
  require 'drb'

  require 'fluentd/engine'
  require 'fluentd/worker'
  require 'fluentd/socket_manager'
  require 'fluentd/server'

  #
  # WorkerLauncher creates, monitor, and restart worker processes.
  #
  # ServerEngine ---> WorkerLauncher#run
  #              ---> spawn("ruby lib/fluentd/command/fluentd-worker.rb")
  #              ---> WorkerLauncher.main
  #
  # To support Windows platform, WorkerLauncher doesn't use fork(). Instead,
  # it runs new processes using 'fluentd/command/fluentd-worker.rb' command.
  # fluentd-worker.rb calls `WorkerLauncher.main`.
  #
  # Next step: `Worker#run` in 'fluentd/worker.rb'
  #
  module WorkerLauncher
    def initialize
      @pm = ServerEngine::ProcessManager.new

      @stop_flag = ServerEngine::BlockingFlag.new

      # command line to start the new process:
      #   $ ruby fluentd/command/fluentd-worker.rb $(drb_uri)
      @cmdline = [
        RbConfig.ruby,
        File.expand_path(File.join(File.dirname(__FILE__), 'command', 'fluentd-worker.rb')),
        worker_id.to_s,
        server.drb_uri,
      ]

      super
    end

    # TODO before_fork can't do blocking operation
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
          logger.info "Process #{worker_id} exited unexpectedly. Restarting."
          first = false
          # TODO stop Server by calling Server#stop_by_config_error
          # if first==true and worker failed to configure. maybe it needs TupleSpace
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
      logger.info "Starting process #{worker_id}"

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

      # Get config from the parent process.
      # See also Server#before_run
      parent_engine = DRb::DRbObject.new_with_uri(drb_uri)

      Engine.shared_data = parent_engine.shared_data
      options = Engine.shared_data['fluentd_options']

      # add :load_path set by cmdline
      $LOAD_PATH.concat options[:load_path]

      # initialize worker instance
      worker = Worker.new(options)
      install_signal_handlers(worker)

      # receive configuration from the shared data through DRb
      conf = Engine.shared_data['fluentd_config']

      # get <process> element for this worker process
      process_elements = conf.elements.select {|e| e.name == 'process' }
      process_element = process_elements[worker_id]
      process_element['id'] ||= worker_id

      # set process name
      # :worker_process_name is set by command/fluentd.rb
      $0 = "#{options[:worker_process_name]} #{process_element['id']}"

      # change user and group
      chuser = process_element['chuser'] || options[:chuser]
      chgroup = process_element['chgroup'] || options[:chgroup]
      ServerEngine::Daemon.change_privilege(chuser, chgroup)

      # configure worker using the config file
      worker.configure(process_element)

      # this worker is ready to run
      # TODO apparently this is not passed to the parent process
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

