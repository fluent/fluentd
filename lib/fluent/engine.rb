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

require 'socket'

require 'cool.io'

require 'fluent/config'
require 'fluent/event'
require 'fluent/event_router'
require 'fluent/msgpack_factory'
require 'fluent/root_agent'
require 'fluent/time'
require 'fluent/system_config'
require 'fluent/plugin'

module Fluent
  class EngineClass
    include Fluent::MessagePackFactory::Mixin

    def initialize
      @root_agent = nil
      @event_router = nil
      @default_loop = nil
      @engine_stopped = false

      @log_emit_thread = nil
      @log_event_loop_stop = false
      @log_event_queue = []

      @suppress_config_dump = false

      @system_config = SystemConfig.new
    end

    MAINLOOP_SLEEP_INTERVAL = 0.3

    MATCH_CACHE_SIZE = 1024
    LOG_EMIT_INTERVAL = 0.1

    attr_reader :root_agent
    attr_reader :matches, :sources
    attr_reader :system_config

    def init(system_config)
      @system_config = system_config

      BasicSocket.do_not_reverse_lookup = true

      suppress_interval(system_config.emit_error_log_interval) unless system_config.emit_error_log_interval.nil?
      @suppress_config_dump = system_config.suppress_config_dump unless system_config.suppress_config_dump.nil?
      @without_source = system_config.without_source unless system_config.without_source.nil?

      @root_agent = RootAgent.new(log: log, system_config: @system_config)

      MessagePackFactory.init

      self
    end

    def log
      $log
    end

    def suppress_interval(interval_time)
      @suppress_emit_error_log_interval = interval_time
      @next_emit_error_log_time = Time.now.to_i
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
          $log.warn message
          next
        end
        unless e.name == 'system'
          unless @without_source && e.name == 'source'
            $log.warn "parameter '#{key}' in #{e.to_s.strip} is not used."
          end
        end
      }
    end

    def configure(conf)
      # plugins / configuration dumps
      Gem::Specification.find_all.select{|x| x.name =~ /^fluent(d|-(plugin|mixin)-.*)$/}.each do |spec|
        $log.info "gem '#{spec.name}' version '#{spec.version}'"
      end

      @root_agent.configure(conf)
      @event_router = @root_agent.event_router

      unless @suppress_config_dump
        $log.info "using configuration file: #{conf.to_s.rstrip}"
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

    def log_event_loop
      $log.disable_events(Thread.current)

      while sleep(LOG_EMIT_INTERVAL)
        break if @log_event_loop_stop
        next if @log_event_queue.empty?

        # NOTE: thead-safe of slice! depends on GVL
        events = @log_event_queue.slice!(0..-1)
        next if events.empty?

        events.each {|tag,time,record|
          begin
            @event_router.emit(tag, time, record)
          rescue => e
            $log.error "failed to emit fluentd's log event", tag: tag, event: record, error: e
          end
        }
      end
    end

    def run
      begin
        start

        if @event_router.match?($log.tag)
          $log.enable_event
          @log_emit_thread = Thread.new(&method(:log_event_loop))
        end

        sleep MAINLOOP_SLEEP_INTERVAL until @engine_stopped

      rescue Exception => e
        $log.error "unexpected error", error: e
        $log.error_backtrace
        raise
      end

      $log.info "shutting down fluentd"
      shutdown
      if @log_emit_thread
        @log_event_loop_stop = true
        @log_emit_thread.join
      end
    end

    def stop
      @engine_stopped = true
      nil
    end

    def push_log_event(tag, time, record)
      return if @log_emit_thread.nil?
      @log_event_queue.push([tag, time, record])
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
