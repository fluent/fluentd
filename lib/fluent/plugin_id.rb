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

require 'set'

module Fluent
  module PluginId
    @@configured_ids = Set.new

    def initialize
      super
      @_plugin_root_dir = nil
    end

    def configure(conf)
      @id = conf['@id']
      @_id_configured = !!@id # plugin id is explicitly configured by users (or not)
      if @id
        @id = @id.to_s
        if @@configured_ids.include?(@id) && !plugin_id_for_test?
          raise Fluent::ConfigError, "Duplicated plugin id `#{@id}`. Check whole configuration and fix it."
        end
        @@configured_ids.add(@id)
      end

      super
    end

    def plugin_id_for_test?
      caller_locations.each do |location|
        # Thread::Backtrace::Location#path returns base filename or absolute path.
        # #absolute_path returns absolute_path always.
        # https://bugs.ruby-lang.org/issues/12159
        if location.absolute_path =~ /\/test_[^\/]+\.rb$/ # location.path =~ /test_.+\.rb$/
          return true
        end
      end
      false
    end

    def plugin_id_configured?
      if instance_variable_defined?("@_id_configured")
        @_id_configured
      end
    end

    def plugin_id
      if instance_variable_defined?("@id")
        @id || "object:#{object_id.to_s(16)}"
      else
        "object:#{object_id.to_s(16)}"
      end
    end

    def plugin_root_dir
      return @_plugin_root_dir if @_plugin_root_dir
      return nil unless system_config.root_dir
      return nil unless plugin_id_configured?

      # Fluent::Plugin::Base#fluentd_worker_id
      dir = File.join(system_config.root_dir, "worker#{fluentd_worker_id}", plugin_id)
      FileUtils.mkdir_p(dir) unless Dir.exist?(dir)
      @_plugin_root_dir = dir.freeze
      dir
    end
  end
end
