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
require 'fluent/config/element'

module Fluent
  class SystemConfig
    include Configurable

    SYSTEM_CONFIG_PARAMETERS = [
      :workers, :root_dir, :log_level,
      :suppress_repeated_stacktrace, :emit_error_log_interval, :suppress_config_dump,
      :log_event_verbose,
      :without_source, :rpc_endpoint, :enable_get_dump, :process_name,
      :file_permission, :dir_permission, :counter_server, :counter_client,
      :strict_config_value
    ]

    config_param :workers,   :integer, default: 1
    config_param :root_dir,  :string, default: nil
    config_param :log_level, :enum, list: [:trace, :debug, :info, :warn, :error, :fatal], default: nil
    config_param :suppress_repeated_stacktrace, :bool, default: nil
    config_param :emit_error_log_interval,      :time, default: nil
    config_param :suppress_config_dump, :bool, default: nil
    config_param :log_event_verbose,    :bool, default: nil
    config_param :without_source,  :bool, default: nil
    config_param :rpc_endpoint,    :string, default: nil
    config_param :enable_get_dump, :bool, default: nil
    config_param :process_name,    :string, default: nil
    config_param :strict_config_value, :bool, default: nil
    config_param :file_permission, default: nil do |v|
      v.to_i(8)
    end
    config_param :dir_permission, default: nil do |v|
      v.to_i(8)
    end
    config_section :log, required: false, init: true, multi: false do
      config_param :format, :enum, list: [:text, :json], default: :text
      config_param :time_format, :string, default: '%Y-%m-%d %H:%M:%S %z'
    end

    config_section :counter_server, multi: false do
      desc 'scope name of counter server'
      config_param :scope, :string

      desc 'the port of counter server to listen to'
      config_param :port, :integer, default: nil
      desc 'the bind address of counter server to listen to'
      config_param :bind, :string, default: nil

      desc 'backup file path of counter values'
      config_param :backup_path, :string
    end

    config_section :counter_client, multi: false do
      desc 'the port of counter server'
      config_param :port, :integer, default: nil
      desc 'the IP address or hostname of counter server'
      config_param :host, :string
      desc 'the timeout of each operation'
      config_param :timeout, :time, default: nil
    end

    def self.create(conf, strict_config_value: false)
      systems = conf.elements(name: 'system')
      return SystemConfig.new if systems.empty?
      raise Fluent::ConfigError, "<system> is duplicated. <system> should be only one" if systems.size > 1

      SystemConfig.new(systems.first, strict_config_value)
    end

    def self.blank_system_config
      Fluent::Config::Element.new('<SYSTEM>', '', {}, [])
    end

    def self.overwrite_system_config(hash)
      older = defined?($_system_config) ? $_system_config : nil
      begin
        $_system_config = SystemConfig.new(Fluent::Config::Element.new('system', '', hash, []))
        yield
      ensure
        $_system_config = older
      end
    end

    def initialize(conf=nil, strict_config_value=false)
      super()
      conf ||= SystemConfig.blank_system_config
      configure(conf, strict_config_value: strict_config_value)
    end

    def configure(conf, strict_config_value: false)
      strict = strict_config_value
      if !strict && conf && conf.has_key?("strict_config_value")
        strict = Fluent::Config.bool_value(conf["strict_config_value"])
      end

      begin
        super(conf, strict_config_value: strict)
      rescue ConfigError => e
        $log.error "config error in:\n#{conf}"
        $log.error 'config error', error: e
        $log.debug_backtrace
        exit!(1)
      end

      @log_level = Log.str_to_level(@log_level.to_s) if @log_level
    end

    def dup
      s = SystemConfig.new
      SYSTEM_CONFIG_PARAMETERS.each do |param|
        s.__send__("#{param}=", instance_variable_get("@#{param}"))
      end
      s
    end

    def overwrite_variables(**opt)
      SYSTEM_CONFIG_PARAMETERS.each do |param|
        if opt.key?(param) && !opt[param].nil? && instance_variable_defined?("@#{param}")
          instance_variable_set("@#{param}", opt[param])
        end
      end
    end

    module Mixin
      def system_config
        require 'fluent/engine'
        unless defined?($_system_config)
          $_system_config = nil
        end
        (instance_variable_defined?("@_system_config") && @_system_config) ||
          $_system_config || Fluent::Engine.system_config
      end

      def system_config_override(opts={})
        require 'fluent/engine'
        if !instance_variable_defined?("@_system_config") || @_system_config.nil?
          @_system_config = (defined?($_system_config) && $_system_config ? $_system_config : Fluent::Engine.system_config).dup
        end
        opts.each_pair do |key, value|
          @_system_config.__send__(:"#{key.to_s}=", value)
        end
      end
    end
  end
end
