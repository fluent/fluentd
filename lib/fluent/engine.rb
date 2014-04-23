#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent
  class EngineClass
    def initialize
      @matches = []
      @sources = []
      @match_cache = {}
      @match_cache_keys = []
      @started = []
      @default_loop = nil
      @engine_stopped = false

      @log_emit_thread = nil
      @log_event_loop_stop = false
      @log_event_queue = []

      @suppress_emit_error_log_interval = 0
      @next_emit_error_log_time = nil

      @suppress_config_dump = false
    end

    MATCH_CACHE_SIZE = 1024

    LOG_EMIT_INTERVAL = 0.1

    attr_reader :matches, :sources

    def init
      BasicSocket.do_not_reverse_lookup = true
      Plugin.load_plugins
      if defined?(Encoding)
        Encoding.default_internal = 'ASCII-8BIT' if Encoding.respond_to?(:default_internal)
        Encoding.default_external = 'ASCII-8BIT' if Encoding.respond_to?(:default_external)
      end
      self
    end

    def suppress_interval(interval_time)
      @suppress_emit_error_log_interval = interval_time
      @next_emit_error_log_time = Time.now.to_i
    end

    def suppress_config_dump=(flag)
      @suppress_config_dump = flag
    end

    def read_config(path)
      $log.info "reading config file", :path=>path
      File.open(path) {|io|
        parse_config(io, File.basename(path), File.dirname(path))
      }
    end

    def parse_config(io, fname, basepath = Dir.pwd, v1_config = false)
      conf = if fname =~ /\.rb$/
               require 'fluent/config/dsl'
               Config::DSL::Parser.parse(io, File.join(basepath, fname))
             else
               Config.parse(io, fname, basepath, v1_config)
             end
      configure(conf)
      conf.check_not_fetched {|key,e|
        $log.warn "parameter '#{key}' in #{e.to_s.strip} is not used."
      }
    end

    def configure(conf)
      # plugins / configuration dumps
      Gem::Specification.find_all.select{|x| x.name =~ /^fluent(d|-(plugin|mixin)-.*)$/}.each do |spec|
        $log.info "gem '#{spec.name}' version '#{spec.version}'"
      end

      unless @suppress_config_dump
        $log.info "using configuration file: #{conf.to_s.rstrip}"
      end

      conf.elements.select {|e|
        e.name == 'source'
      }.each {|e|
        type = e['type']
        unless type
          raise ConfigError, "Missing 'type' parameter on <source> directive"
        end
        $log.info "adding source type=#{type.dump}"

        input = Plugin.new_input(type)
        input.configure(e)

        @sources << input
      }

      conf.elements.select {|e|
        e.name == 'match'
      }.each {|e|
        type = e['type']
        pattern = e.arg
        unless type
          raise ConfigError, "Missing 'type' parameter on <match #{e.arg}> directive"
        end
        $log.info "adding match", :pattern=>pattern, :type=>type

        output = Plugin.new_output(type)
        output.configure(e)

        match = Match.new(pattern, output)
        @matches << match
      }
    end

    def load_plugin_dir(dir)
      Plugin.load_plugin_dir(dir)
    end

    def emit(tag, time, record)
      unless record.nil?
        emit_stream tag, OneEventStream.new(time, record)
      end
    end

    def emit_array(tag, array)
      emit_stream tag, ArrayEventStream.new(array)
    end

    def emit_stream(tag, es)
      target = @match_cache[tag]
      unless target
        target = match(tag) || NoMatchMatch.new
        # this is not thread-safe but inconsistency doesn't
        # cause serious problems while locking causes.
        if @match_cache_keys.size >= MATCH_CACHE_SIZE
          @match_cache.delete @match_cache_keys.shift
        end
        @match_cache[tag] = target
        @match_cache_keys << tag
      end
      target.emit(tag, es)
    rescue => e
      if @suppress_emit_error_log_interval == 0 || now > @next_emit_error_log_time
        $log.warn "emit transaction failed ", :error_class=>e.class, :error=>e
        $log.warn_backtrace
        # $log.debug "current next_emit_error_log_time: #{Time.at(@next_emit_error_log_time)}"
        @next_emit_error_log_time = Time.now.to_i + @suppress_emit_error_log_interval
        # $log.debug "next emit failure log suppressed"
        # $log.debug "next logged time is #{Time.at(@next_emit_error_log_time)}"
      end
      raise
    end

    def match(tag)
      @matches.find {|m| m.match(tag) }
    end

    def match?(tag)
      !!match(tag)
    end

    def flush!
      flush_recursive(@matches)
    end

    def now
      # TODO thread update
      Time.now.to_i
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
            Engine.emit(tag, time, record)
          rescue => e
            $log.error "failed to emit fluentd's log event", :tag => tag, :event => record, :error_class => e.class, :error => e
          end
        }
      end
    end

    def run
      begin
        start

        if match?($log.tag)
          $log.enable_event
          @log_emit_thread = Thread.new(&method(:log_event_loop))
        end

        unless @engine_stopped
          # for empty loop
          @default_loop = Coolio::Loop.default
          @default_loop.attach Coolio::TimerWatcher.new(1, true)
          # TODO attach async watch for thread pool
          @default_loop.run
        end

        if @engine_stopped and @default_loop
          @default_loop.stop
          @default_loop = nil
        end

      rescue => e
        $log.error "unexpected error", :error_class=>e.class, :error=>e
        $log.error_backtrace
      ensure
        $log.info "shutting down fluentd"
        unless $platformwin
          shutdown
        else
          shutdown_win
        end
        if @log_emit_thread
          @log_event_loop_stop = true
          @log_emit_thread.join
        end
      end
    end

    def stop
      @engine_stopped = true
      if @default_loop
        @default_loop.stop
        @default_loop = nil
      end
      nil
    end

    def push_log_event(tag, time, record)
      return if @log_emit_thread.nil?
      @log_event_queue.push([tag, time, record])
    end

    private
    def start
      @matches.each {|m|
        m.start
        @started << m
      }
      @sources.each {|s|
        s.start
        @started << s
      }
    end

    def shutdown
      @started.map {|s|
        Thread.new do
          begin
            s.shutdown
          rescue => e
            $log.warn "unexpected error while shutting down", :error_class=>e.class, :error=>e
            $log.warn_backtrace
          end
        end
      }.each {|t|
        t.join
      }
    end

    def shutdown_win
      @started.map {|s|
        begin
          s.shutdown
        rescue => e
          $log.warn "unexpected error while shutting down", :error_class=>e.class, :error=>e
          $log.warn_backtrace
        end
    }
  end

    def flush_recursive(array)
      array.each {|m|
        begin
          if m.is_a?(Match)
            m = m.output
          end
          if m.is_a?(BufferedOutput)
            m.force_flush
          elsif m.is_a?(MultiOutput)
            flush_recursive(m.outputs)
          end
        rescue => e
          $log.debug "error while force flushing", :error_class=>e.class, :error=>e
          $log.debug_backtrace
        end
      }
    end

    class NoMatchMatch
      def initialize
        @count = 0
      end

      def emit(tag, es)
        # TODO use time instead of num of records
        c = (@count += 1)
        if c < 512
          if Math.log(c) / Math.log(2) % 1.0 == 0
            $log.warn "no patterns matched", :tag=>tag
            return
          end
        else
          if c % 512 == 0
            $log.warn "no patterns matched", :tag=>tag
            return
          end
        end
        $log.on_trace { $log.trace "no patterns matched", :tag=>tag }
      end

      def start
      end

      def shutdown
      end

      def match(tag)
        false
      end
    end
  end

  Engine = EngineClass.new


  module Test
    @@test = false

    def test?
      @@test
    end

    def self.setup
      @@test = true

      Fluent.__send__(:remove_const, :Engine)
      engine = Fluent.const_set(:Engine, EngineClass.new).init

      engine.define_singleton_method(:now=) {|n|
        @now = n.to_i
      }
      engine.define_singleton_method(:now) {
        @now || super()
      }

      nil
    end
  end
end

