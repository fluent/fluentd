#
# Fluent
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

require 'fluent/configurable'

module Fluent
  module SystemConfigMixin
    def system_config
      @_system_config || Fluent::Engine.system_config
    end

    def system_config_override(opts={})
      unless @_system_config
        @_system_config = Fluent::Engine.system_config.dup
      end
      opts.each_pair do |key, value|
        @_system_config.send(:"#{key.to_s}=", value)
      end
    end
  end

  class SystemConfig
    include Configurable

    config_param :log_level, default: nil do |level|
      Log.str_to_level(level)
    end
    config_param :suppress_repeated_stacktrace, :bool, default: nil
    config_param :emit_error_log_interval, :time, default: nil
    config_param :suppress_config_dump, :bool, default: nil
    config_param :without_source, :bool, default: nil
    config_param :plugin_storage_path, :string, default: nil

    def self.create(conf)
      systems = conf.elements.select { |e|
        e.name == 'system'
      }
      return SystemConfig.new({}) if systems.empty?
      raise Fluent::ConfigError, "<system> is duplicated. <system> should be only one" if systems.size > 1

      SystemConfig.new(systems.first)
    end

    def initialize(conf)
      super()
      configure(conf)
    end

    def dup
      s = SystemConfig.new({})
      s.log_level = @log_level
      s.suppress_repeated_stacktrace = @suppress_repeated_stacktrace
      s.emit_error_log_interval = @emit_error_log_interval
      s.suppress_config_dump = @suppress_config_dump
      s.without_source = @without_source
      s.plugin_storage_path = @plugin_storage_path

      s
    end

    def apply(supervisor)
      system = self
      supervisor.instance_eval {
        @log.level = @log_level = system.log_level unless system.log_level.nil?
        @suppress_interval = system.emit_error_log_interval unless system.emit_error_log_interval.nil?
        @suppress_config_dump = system.suppress_config_dump unless system.suppress_config_dump.nil?
        @suppress_repeated_stacktrace = system.suppress_repeated_stacktrace unless system.suppress_repeated_stacktrace.nil?
        @without_source = system.without_source unless system.without_source.nil?
        @plugin_storage_path = system.plugin_storage_path unless system.plugin_storage_path.nil?
      }
    end
  end
end
