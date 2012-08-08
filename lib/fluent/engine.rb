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
module Fluent

  class EngineClass
    def self.setup!(collector, plugin)
      require 'fluent/load'
      Fluent.const_set(:Engine, EngineClass.new(collector))
      Fluent.const_set(:Plugin, PluginClass.new(plugin)).load_plugins
      $log = Log.new
    end

    def initialize(collector)
      @collector = collector
    end

    def emit(tag, time, record)
      w = @collector.open
      begin
        w.append(tag, time, record)
      ensure
        w.close
      end
    end

    def emit_array(tag, array)
      w = @collector.open
      begin
        array.each {|time,record|
          w.append(tag, time, record)
        }
      ensure
        w.close
      end
    end

    def emit_stream(tag, es)
      w = @collector.open
      begin
        w.write(es)
      ensure
        w.close
      end
    end

    def now
      # TODO thread update
      Time.now.to_i
    end
  end


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

