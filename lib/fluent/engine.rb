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
  end

  def init
    $log.info "started at #{Time.now}"
    require 'thread'
    require 'monitor'
    require 'stringio'
    require 'fileutils'
    require 'json'
    require 'eventmachine'
    require 'fluent/env'
    require 'fluent/config'
    require 'fluent/plugin'
    require 'fluent/parser'
    require 'fluent/event'
    require 'fluent/buffer'
    require 'fluent/input'
    require 'fluent/output'
    require 'fluent/match'
    Plugin.load_built_in_plugin
    self
  end

  def read_config(path)
    $log.info "reading config file '#{path}'"
    conf = Config.read(path)
    configure(conf)
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
      $log.info "adding match pattern=#{pattern.dump} type=#{type.dump}"

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
    emit_stream tag, ArrayEventStream.new([event])
  end

  def emit_array(tag, array)
    emit_stream tag, ArrayEventStream.new(array)
  end

  def emit_stream(tag, es)
    if match = @matches.find {|m| m.match(tag) }
      match.emit(tag, es)
    else
      $log.trace { "no pattern matched: tag=#{tag}" }
    end
  rescue
    $log.on_warn {
      $log.warn "emit transaction faild: ", $!
      $log.warn_backtrace
    }
    raise
  end

  def now
    # TODO thread update
    Time.now.to_i
  end

  def run
    EventMachine.run do
      start
    end
    shutdown
    nil
  end

  def stop
    EventMachine.stop_event_loop
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
end

Engine = EngineClass.new


end

