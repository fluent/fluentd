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

require 'fluent/config'
require 'fluent/event'
require 'fluent/event_router'
require 'fluent/msgpack_factory'
require 'fluent/root_agent'
require 'fluent/time'
require 'fluent/system_config'
require 'fluent/plugin'
require 'fluent/fluent_log_event_router'

module Fluent
  class EngineClass
    def initialize
      @root_agent = nil
      @default_loop = nil
      @engine_stopped = false
      @_worker_id = nil

      @fluent_log_event_router = nil
      @log_event_verbose = false

      @suppress_config_dump = false

      @system_config = SystemConfig.new

      @dry_run_mode = false
    end

    MAINLOOP_SLEEP_INTERVAL = 0.3

    attr_reader :root_agent, :system_config
    attr_accessor :dry_run_mode

    def init(system_config)
      @system_config = system_config

      @suppress_config_dump = system_config.suppress_config_dump unless system_config.suppress_config_dump.nil?
      @without_source = system_config.without_source unless system_config.without_source.nil?

      @log_event_verbose = system_config.log_event_verbose unless system_config.log_event_verbose.nil?

      @root_agent = RootAgent.new(log: log, system_config: @system_config)

      self
    end

    def log
      $log
    end

    def parse_config(io, fname, basepath = Dir.pwd, v1_config = false)
      if fname =~ /\.rb$/
        require 'fluent/config/dsl'
        Config::DSL::Parser.parse(io, File.join(basepath, fname))
      else
        Config.parse(io, fname, basepath, v1_config)
      end
    end

    def run_configure(conf)
      configure(conf)
      conf.check_not_fetched { |key, e|
        parent_name, plugin_name = e.unused_in
        if parent_name
          message = if plugin_name
                      "section <#{e.name}> is not used in <#{parent_name}> of #{plugin_name} plugin"
                    else
                      "section <#{e.name}> is not used in <#{parent_name}>"
                    end
          if e.for_every_workers?
            $log.warn :worker0, message
          elsif e.for_this_worker?
            $log.warn message
          end
          next
        end
        unless e.name == 'system'
          unless @without_source && e.name == 'source'
            message = "parameter '#{key}' in #{e.to_s.strip} is not used."
            if e.for_every_workers?
              $log.warn :worker0, message
            elsif e.for_this_worker?
              $log.warn message
            end
          end
        end
      }
    end

    def configure(conf)
      @root_agent.configure(conf)

      @fleunt_log_event_router = FluentLogEventRouter.build(@root_agent)

      if @fleunt_log_event_router
        $log.enable_event(true)
      end

      unless @suppress_config_dump
        $log.info :supervisor, "using configuration file: #{conf.to_s.rstrip}"
      end
    end

    def add_plugin_dir(dir)
      Plugin.add_plugin_dir(dir)
    end

    def emit(tag, time, record)
      raise "BUG: use router.emit instead of Engine.emit"
    end

    def emit_array(tag, array)
      raise "BUG: use router.emit_array instead of Engine.emit_array"
    end

    def emit_stream(tag, es)
      raise "BUG: use router.emit_stream instead of Engine.emit_stream"
    end

    def flush!
      @root_agent.flush!
    end

    def now
      # TODO thread update
      Fluent::EventTime.now
    end

    def run
      begin
        $log.info "starting fluentd worker", pid: Process.pid, ppid: Process.ppid, worker: worker_id
        start

        @fleunt_log_event_router.start

        $log.info "fluentd worker is now running", worker: worker_id
        sleep MAINLOOP_SLEEP_INTERVAL until @engine_stopped
        $log.info "fluentd worker is now stopping", worker: worker_id

      rescue Exception => e
        $log.error "unexpected error", error: e
        $log.error_backtrace
        raise
      end

      unless @log_event_verbose
        $log.enable_event(false)
        @fleunt_log_event_router.graceful_stop
      end
      $log.info "shutting down fluentd worker", worker: worker_id
      shutdown

      @fleunt_log_event_router.stop
    end

    def stop
      @engine_stopped = true
      nil
    end

    def push_log_event(tag, time, record)
      @fleunt_log_event_router.emit_event([tag, time, record])
    end

    def worker_id
      return @_worker_id if @_worker_id
      # if ENV doesn't have SERVERENGINE_WORKER_ID, it is a worker under --no-supervisor or in tests
      # so it's (almost) a single worker, worker_id=0
      @_worker_id = (ENV['SERVERENGINE_WORKER_ID'] || 0).to_i
      @_worker_id
    end

    private
    def start
      @root_agent.start
    end

    def shutdown
      @root_agent.shutdown
    end
  end

  Engine = EngineClass.new
end
