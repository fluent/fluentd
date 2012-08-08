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

  class PluginClass
    def initialize(plugin)
      @plugin = plugin
      @buffers = {}
    end

    def register_input(type, klass)
      klass = InputBackwardCompatWrapper::Factory.new(klass)
      @plugin.register_input(type, klass)
    end

    def register_output(type, klass)
      klass = OutputBackwardCompatWrapper::Factory.new(klass)
      @plugin.register_output(type, klass)
    end

    def register_buffer(type, klass)
      @buffers[type] = klass
    end

    def new_input(type)
      o = @plugin.new_input(type)
      InputForwardCompatWrapper.new(o)
    end

    def new_output(type)
      o = @plugin.new_output(type)
      OutputForwardCompatWrapper.new(o)
    end

    def new_buffer(type)
      if klass = map[type]
        return klass.new
      end
      raise ConfigError, "Unknown buffer plugin '#{type}'."
    end

    def load_plugins
      dir = File.join(File.dirname(__FILE__), "plugin")
      load_plugin_dir(dir)
      #load_gem_plugins
    end

    def load_plugin_dir(dir)
      dir = File.expand_path(dir)
      Dir.entries(dir).sort.each {|fname|
        if fname =~ /\.rb$/
          require File.join(dir, fname)
        end
      }
      nil
    end
  end

end

