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
  end

  def init
    require 'fluent/load'
    BasicSocket.do_not_reverse_lookup = true
    Plugin.load_plugins
    Encoding.default_internal = 'ASCII-8BIT' if defined?(Encoding) && Encoding.respond_to?(:default_internal)
    Encoding.default_external = 'ASCII-8BIT' if defined?(Encoding) && Encoding.respond_to?(:default_external)
    self
  end

  def read_config(path)
    $log.info "reading config file", :path=>path
    conf = Config.read(path)
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

  def emit(tag, event)
    emit_stream tag, OneEventStream.new(event)
  end

  def emit_array(tag, array)
    emit_stream tag, ArrayEventStream.new(array)
  end

  def emit_stream(tag, es)
    match = @match_cache[tag]
    unless match
      match = @matches.find {|m| m.match(tag) } || NoMatchMatch.new
      if @match_cache.size < 1024  # TODO size limit
        @match_cache[tag] = match
      end
    end
    match.emit(tag, es)
  rescue
    $log.warn "emit transaction faild ", :error=>$!.to_s
    $log.warn_backtrace
    raise
  end

  def match?(tag)
    @matches.find {|m| m.match(tag) }
  end

  def flush!
    flush_recursive(@matches)
  end

  def now
    # TODO thread update
    Time.now.to_i
  end

  def run
    start

    if match?($log.tag)
      $log.enable_event
    end

    # for empty loop
    Coolio::Loop.default.attach Coolio::TimerWatcher.new(1, true)
    # TODO attach async watch for thread pool
    Coolio::Loop.default.run

    shutdown
  end

  def stop
    $log.info "shutting down fluentd"
    Coolio::Loop.default.stop
    nil
  end

  private
  def start
    @matches.each {|m|
      m.start
    }
    @sources.each {|s|
      s.start
    }
  end

  def shutdown
    @matches.each {|m|
      m.shutdown rescue nil
    }
    @sources.each {|s|
      s.shutdown rescue nil
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


end

