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
require 'fluent/static_config_analysis'

module Fluent
  class EngineClass
    # For compat. remove it in fluentd v2
    include Fluent::MessagePackFactory::Mixin

    def initialize
      @root_agent = nil
      @engine_stopped = false
      @_worker_id = nil

      @log_event_verbose = false
      @suppress_config_dump = false
      @without_source = false

      @fluent_log_event_router = nil
      @system_config = SystemConfig.new

      @supervisor_mode = false

      @root_agent_mutex = Mutex.new
    end

    MAINLOOP_SLEEP_INTERVAL = 0.3

    attr_reader :root_agent, :system_config, :supervisor_mode

    def init(system_config, supervisor_mode: false, start_in_parallel: false)
      @system_config = system_config
      @supervisor_mode = supervisor_mode

      @suppress_config_dump = system_config.suppress_config_dump unless system_config.suppress_config_dump.nil?
      @without_source = system_config.without_source unless system_config.without_source.nil?

      @log_event_verbose = system_config.log_event_verbose unless system_config.log_event_verbose.nil?

      @root_agent = RootAgent.new(log: log, system_config: @system_config, start_in_parallel: start_in_parallel)

      self
    end

    def log
      $log
    end

    def parse_config(io, fname, basepath = Dir.pwd, v1_config = false)
      if /\.rb$/.match?(fname)
        require 'fluent/config/dsl'
        Config::DSL::Parser.parse(io, File.join(basepath, fname))
      else
        Config.parse(io, fname, basepath, v1_config)
      end
    end

    def run_configure(conf, dry_run: false)
      configure(conf)
      conf.check_not_fetched do |key, e|
        parent_name, plugin_name = e.unused_in
        message = if parent_name && plugin_name
                    "section <#{e.name}> is not used in <#{parent_name}> of #{plugin_name} plugin"
                  elsif parent_name
                    "section <#{e.name}> is not used in <#{parent_name}>"
                  elsif e.name != 'system' && !(@without_source && e.name == 'source')
                    "parameter '#{key}' in #{e.to_s.strip} is not used."
                  else
                    nil
                  end
        next if message.nil?

        if dry_run && @supervisor_mode
          $log.warn :supervisor, message
        elsif e.for_every_workers?
          $log.warn :worker0, message
        elsif e.for_this_worker?
          $log.warn message
        end
      end
    end

    def configure(conf)
      @root_agent.configure(conf)

      @fluent_log_event_router = FluentLogEventRouter.build(@root_agent)

      if @fluent_log_event_router.emittable?
        $log.enable_event(true)
      end

      unless @suppress_config_dump
        $log.info :supervisor, "using configuration file: #{conf.to_s.rstrip}"
      end
    end

    def add_plugin_dir(dir)
      $log.warn('Deprecated method: this method is going to be deleted. Use Fluent::Plugin.add_plugin_dir')
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
      @root_agent_mutex.synchronize do
        @root_agent.flush!
      end
    end

    def cancel_source_only!
      @root_agent_mutex.synchronize do
        @root_agent.cancel_source_only!
      end
    end

    def now
      # TODO thread update
      Fluent::EventTime.now
    end

    def run
      begin
        @fluent_log_event_router.start

        $log.info "starting fluentd worker", pid: Process.pid, ppid: Process.ppid, worker: worker_id
        @root_agent_mutex.synchronize do
          start
        end

        $log.info "fluentd worker is now running", worker: worker_id
        sleep MAINLOOP_SLEEP_INTERVAL until @engine_stopped
        $log.info "fluentd worker is now stopping", worker: worker_id

      rescue Exception => e
        $log.error "unexpected error", error: e
        $log.error_backtrace
        raise
      end

      @root_agent_mutex.synchronize do
        stop_phase(@root_agent)
      end
    end

    # @param conf [Fluent::Config]
    # @param supervisor [Bool]
    # @return nil
    def reload_config(conf, supervisor: false)
      @root_agent_mutex.synchronize do
        # configure first to reduce down time while restarting
        new_agent = RootAgent.new(log: log, system_config: @system_config)
        ret = Fluent::StaticConfigAnalysis.call(conf, workers: system_config.workers)

        ret.all_plugins.each do |plugin|
          if plugin.respond_to?(:reloadable_plugin?) && !plugin.reloadable_plugin?
            raise Fluent::ConfigError, "Unreloadable plugin plugin: #{Fluent::Plugin.lookup_type_from_class(plugin.class)}, plugin_id: #{plugin.plugin_id}, class_name: #{plugin.class})"
          end
        end

        # Assign @root_agent to new root_agent
        # for https://github.com/fluent/fluentd/blob/fcef949ce40472547fde295ddd2cfe297e1eddd6/lib/fluent/plugin_helper/event_emitter.rb#L50
        old_agent, @root_agent = @root_agent, new_agent
        begin
          @root_agent.configure(conf)
        rescue
          @root_agent = old_agent
          raise
        end

        unless @suppress_config_dump
          $log.info :supervisor, "using configuration file: #{conf.to_s.rstrip}"
        end

        # supervisor doesn't handle actual data. so the following code is unnecessary.
        if supervisor
          old_agent.shutdown      # to close thread created in #configure
          return
        end

        stop_phase(old_agent)

        $log.info 'restart fluentd worker', worker: worker_id
        start_phase(new_agent)
      end
    end

    def stop
      @engine_stopped = true
      nil
    end

    def push_log_event(tag, time, record)
      @fluent_log_event_router.emit_event([tag, time, record])
    end

    def worker_id
      if @supervisor_mode
        return -1
      end

      return @_worker_id if @_worker_id
      # if ENV doesn't have SERVERENGINE_WORKER_ID, it is a worker under --no-supervisor or in tests
      # so it's (almost) a single worker, worker_id=0
      @_worker_id = (ENV['SERVERENGINE_WORKER_ID'] || 0).to_i
      @_worker_id
    end

    private

    def stop_phase(root_agent)
      unless @log_event_verbose
        $log.enable_event(false)
        @fluent_log_event_router.graceful_stop
      end
      $log.info 'shutting down fluentd worker', worker: worker_id
      root_agent.shutdown

      @fluent_log_event_router.stop
    end

    def start_phase(root_agent)
      @fluent_log_event_router = FluentLogEventRouter.build(root_agent)
      if @fluent_log_event_router.emittable?
        $log.enable_event(true)
      end

      @root_agent.start
    end

    def start
      @root_agent.start
    end
  end

  Engine = EngineClass.new
end
