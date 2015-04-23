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

require 'uri'
require 'fileutils'
require 'fluent/plugin/buffer'
require 'fluent/plugin/buffer/file_chunk'

module Fluent::Plugin
  class FileBuffer < Buffer
    Fluent::Plugin.register_buffer('file', self)

    @@buffer_paths = {}

    config_section :buffer, param_name: :buffer_config do
      desc 'The path where buffer chunks are stored.'
      config_param :path, :string, default: nil # buffer plugin refers system configuration and generate path for each plugin, if `@id` specified
      desc 'If true, queued chunks are flushed at shutdown process.'
      config_set_default :flush_at_shutdown, :bool, default: false
    end
    # permission?

    ##TODO: Buffer plugin cannot handle symlinks because new API @stage has many writing buffer chunks
    ##      re-implement this feature on out_file, w/ enqueue_chunk(or generate_chunk) hook + chunk.path
    # attr_accessor :symlink_path

    def configure(plugin_id, conf)
      super
      @uri_parser = URI::Parser.new

      buffer_path = @buffer_config.path

      unless buffer_path
        if !plugin_id || !system_config.buffer_dir_path
          raise Fluent::ConfigError, "Cannot determine path for file buffer plugin!"
        end
        buffer_path = File.join(system_config.buffer_dir_path, plugin_id, 'buffer.*.log')
      end

      if @@buffer_paths.has_key?(buffer_path)
        raise Fluent::ConfigError, "Duplicated buffer file path '#{buffer_path}'"
      end
      @@buffer_paths[buffer_path] = self

      @path = if buffer_path.index('*')
                buffer_path
              else
                buffer_path + '.*.log'
              end

      @flush_at_shutdown = @buffer_config.flush_at_shutdown
    end

    def start
      FileUtils.mkdir_p File.dirname(@path), mode: DEFAULT_DIR_PERMISSION
      super
    end

    def resume
      stage = {}
      queue = []

      Dir.glob(@path) do |path|
        m = metadata()
        assumed_state = Fluent::Plugin::Buffer::FileChunk.assume_chunk_state(path)
        mode = case assumed_state
               when :staged then 'r+'
               when :queued then 'r'
               else
                 raise "BUG: unexpected chunk state from assume_chunk_state: #{assumed_state}"
               end
        chunk = Fluent::Plugin::Buffer::FileChunk.new(m, path, mode)

        case chunk.state
        when :staged
          stage[chunk.metadata] = chunk
        when :queued, :blocked
          queue << chunk
        else
          raise "BUG: unexpected chunk state '#{chunk.state}' for path '#{path}'"
        end
      end

      queue.sort_by!{ |chunk| chunk.modified_at }

      return stage, queue
    end

    def generate_chunk(metadata)
      Fluent::Plugin::Buffer::FileChunk.new(metadata, @path, 'w+')
    end

    # overwrite to call `chunk.enqueued!`
    def enqueue_chunk(metadata)
      synchronize do
        chunk = @stage.delete(metadata)
        if chunk
          chunk.synchronize do
            chunk.enqueued!
          end
          @queue << chunk
        end
        nil
      end
    end
  end
end
