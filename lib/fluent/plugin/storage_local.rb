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

require 'fluent/plugin'
require 'fluent/plugin/storage'

require 'fileutils'
require 'yajl'

module Fluent
  module Plugin
    class LocalStorage < Storage
      Fluent::Plugin.register_storage('local', self)

      DEFAULT_DIR_MODE = 0755
      DEFAULT_FILE_MODE = 0644

      config_param :path, :string, default: nil
      config_param :mode, :integer, default: DEFAULT_FILE_MODE
      config_param :dir_mode, :integer, default: DEFAULT_DIR_MODE
      config_param :pretty_print, :bool, default: false

      def initialize
        super
        @store = {}
      end

      def configure(conf)
        super

        @on_memory = false
        if !@path && !@_plugin_id_configured
          if @persistent
            raise Fluent::ConfigError, "Plugin @id or path for <storage> required to save data"
          else
            if @autosave
              log.warn "both of Plugin @id and path for <storage> are not specified. Using on-memory store."
            else
              log.info "both of Plugin @id and path for <storage> are not specified. Using on-memory store."
            end
            @on_memory = true
          end
        elsif @path
          # ok
        else # @_plugin_id_configured is true
          log.warn "path for <storage> is not specified. Using on-memory store temporarily, but will use file store after support global storage path"
          @on_memory = true
          ## TODO: get process-wide directory for plugin storage, and generate path for this plugin storage instance
          # path = 
        end

        if !@on_memory
          dir = File.dirname(@path)
          FileUtils.mkdir_p(dir, mode: @dir_mode) unless File.exist?(dir)
          if File.exist?(@path)
            raise Fluent::ConfigError, "Plugin storage path '#{@path}' is not readable/writable" unless File.readable?(@path) && File.writable?(@path)
            begin
              data = Yajl::Parser.parse(open(@path, 'r:utf-8'){ |io| io.read })
              raise Fluent::ConfigError, "Invalid contents (not object) in plugin storage file: '#{@path}'" unless data.is_a?(Hash)
            rescue => e
              log.error "failed to read data from plugin storage file", path: @path, error: e
              raise Fluent::ConfigError, "Unexpected error: failed to read data from plugin storage file: '#{@path}'"
            end
          else
            raise Fluent::ConfigError, "Directory is not writable for plugin storage file '#{dir}'" unless File.writable?(dir)
          end
        end
      end

      def load
        return if @on_memory
        return unless File.exist?(@path)
        begin
          json_string = open(@path, 'r:utf-8'){ |io| io.read }
          json = Yajl::Parser.parse(json_string)
          unless json.is_a?(Hash)
            log.error "broken content for plugin storage (Hash required: ignored)", type: json.class
            log.debug "broken content", content: json_string
            return
          end
          @store = json
        rescue => e
          log.error "failed to load data for plugin storage from file", path: @path, error: e
        end
      end

      def save
        return if @on_memory
        tmp_path = @path + '.tmp'
        begin
          json_string = Yajl::Encoder.encode(@store, pretty: @pretty_print)
          open(tmp_path, 'w:utf-8', @mode){ |io| io.write json_string }
          File.rename(tmp_path, @path)
        rescue => e
          log.error "failed to save data for plugin storage to file", path: @path, tmp: tmp_path, error: e
        end
      end

      def get(key)
        @store[key.to_s]
      end

      def fetch(key, defval)
        @store.fetch(key.to_s, defval)
      end

      def put(key, value)
        @store[key.to_s] = value
      end

      def delete(key)
        @store.delete(key.to_s)
      end

      def update(key, &block)
        @store[key.to_s] = block.call(@store[key.to_s])
      end
    end
  end
end
