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
    @started = []
    @default_loop = nil
  end

  def init
    BasicSocket.do_not_reverse_lookup = true
    Plugin.load_plugins
    if defined?(Encoding)
      Encoding.default_internal = 'ASCII-8BIT' if Encoding.respond_to?(:default_internal)
      Encoding.default_external = 'ASCII-8BIT' if Encoding.respond_to?(:default_external)
    end
    self
  end

  def read_config(path)
    $log.info "reading config file", :path=>path
    File.open(path) {|io|
      parse_config(io, File.basename(path), File.dirname(path))
    }
  end

  def parse_config(io, fname, basepath=Dir.pwd)
    conf = Config.parse(io, fname, basepath)
    configure(conf)
    conf.check_not_fetched {|key,e|
      $log.warn "parameter '#{key}' in #{e.to_s.strip} is not used."
    }
  end

  def configure(conf)
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
    emit_stream tag, OneEventStream.new(time, record)
  end

  def emit_array(tag, array)
    emit_stream tag, ArrayEventStream.new(array)
  end

  def emit_stream(tag, es)
    target = @match_cache[tag]
    unless target
      target = match(tag) || NoMatchMatch.new
      if @match_cache.size < 1024  # TODO size limit
        @match_cache[tag] = target
      end
    end
    target.emit(tag, es)
  rescue
    $log.warn "emit transaction faild ", :error=>$!.to_s
    $log.warn_backtrace
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

  def run
    begin
      start

      if match?($log.tag)
        $log.enable_event
      end

      # for empty loop
      @default_loop = Coolio::Loop.default
      @default_loop.attach Coolio::TimerWatcher.new(1, true)
      # TODO attach async watch for thread pool
      @default_loop.run

    rescue
      $log.error "unexpected error", :error=>$!.to_s
      $log.error_backtrace
    ensure
      shutdown
    end
  end

  def stop
    $log.info "shutting down fluentd"
    if @default_loop
      @default_loop.stop
      @default_loop = nil
    end
    nil
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
    @started.reverse_each {|s|
      begin
        s.shutdown
      rescue
        $log.warn "unexpected error while shutting down", :error=>$!.to_s
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
          m.try_flush
        elsif m.is_a?(MultiOutput)
          flush_recursive(m.outputs)
        end
      rescue
        $log.debug "error while force flushing", :error=>$!.to_s
        $log.debug_backtrace
      end
    }
  end

  class NoMatchMatch
    def emit(tag, es)
      $log.on_trace { $log.trace "no pattern matched", :tag=>tag }
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

