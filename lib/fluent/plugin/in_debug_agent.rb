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

require 'fluent/plugin/input'

module Fluent::Plugin
  class DebugAgentInput < Input
    Fluent::Plugin.register_input('debug_agent', self)

    def initialize
      require 'drb/drb'
      require 'fluent/plugin/file_util'
      super
    end

    config_param :bind, :string, default: '0.0.0.0'
    config_param :port, :integer, default: 24230
    config_param :unix_path, :string, default: nil
    #config_param :unix_mode  # TODO
    config_param :object, :string, default: 'Fluent::Engine'

    def configure(conf)
      super
      if system_config.workers > 1
        @port += fluentd_worker_id
      end
      if @unix_path
        unless ::Fluent::FileUtil.writable?(@unix_path)
          raise Fluent::ConfigError, "in_debug_agent: `#{@unix_path}` is not writable"
        end
      end
    end

    def multi_workers_ready?
      @unix_path.nil?
    end

    def start
      super

      if @unix_path
        require 'drb/unix'
        uri = "drbunix:#{@unix_path}"
      else
        uri = "druby://#{@bind}:#{@port}"
      end
      log.info "listening dRuby", uri: uri, object: @object, worker: fluentd_worker_id
      obj = eval(@object)
      @server = DRb::DRbServer.new(uri, obj)
    end

    def shutdown
      @server.stop_service if @server

      super
    end
  end
end
