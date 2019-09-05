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

require 'fileutils'

require 'fluent/plugin/buffer'
require 'fluent/plugin/buffer/chunkio_chunk'
require 'fluent/system_config'
require 'chunkio'

module Fluent
  module Plugin
    class ChunkioBuffer < Fluent::Plugin::Buffer
      Plugin.register_buffer('chunkio', self)

      include SystemConfig::Mixin

      DEFAULT_CHUNK_LIMIT_SIZE = 256 * 1024 * 1024        # 256MB
      DEFAULT_TOTAL_LIMIT_SIZE = 64 * 1024 * 1024 * 1024  #  64GB

      DEFAULT_FILE_EXT = 'buf'.freeze
      FILE_NAME = 'cio.*'.freeze
      DIR_PERMISSION = 0755

      desc 'The directory path to store `streams` where buffer chunks are stored.'
      config_param :path, :string, default: nil
      desc 'if multiple worker and nil then `workerN` otherwise default value is `stream`'
      config_param :stream_name, :string, default: 'buffer'
      desc 'The suffix of buffer file'
      config_param :file_suffix, :string, default: DEFAULT_FILE_EXT
      desc 'The permission of chunk directory. If no specified, <system> setting or 0755 is used'
      config_param :dir_permission, :string, default: nil
      # desc 'The permission of chunk file. If no specified, <system> setting or 0644 is used'
      # config_param :file_permission, :string, default: nil

      config_set_default :chunk_limit_size, DEFAULT_CHUNK_LIMIT_SIZE
      config_set_default :total_limit_size, DEFAULT_TOTAL_LIMIT_SIZE

      @@buffer_paths = {}

      def initialize
        super

        @multi_workers_available = false
        @additional_resume_path = nil
      end

      def configure(conf)
        super

        @root_dir = @path || owner.plugin_root_dir

        unless @root_dir
          raise Fluent::ConfigError, "buffer path is not configured. specify 'path' in <buffer>"
        end

        if File.basename(@root_dir).include?('*')
          raise Fluent::ConfigError, "chunkio file can not allow to use '*' parameter, specify path to stream direcotry"
        end

        if @root_dir.empty?
          raise Fluent::ConfigError, 'Path should be at least one charactor. empty string is not allowed'
        end

        if @stream_name.empty?
          raise Fluent::ConfigError, 'stream_name should be at least one charactor. empty string is not allowed'
        end

        @file_name = "#{FILE_NAME}.#{@file_suffix}"
        if owner.system_config.workers > 1
          if fluentd_worker_id == 0
            # worker 0 always checks unflushed buffer chunks to be resumed (might be created while non-multi-worker configuration)
            @additional_resume_path = File.join(@root_dir, @stream_name, @file_name)
            @stream_name_original = @stream_name
          end

          if @path
            # #plugin_root_dir has worker_id
            # https://github.com/fluent/fluentd/blob/75d6f3074d5a66e91f03aa45b6bfd1f3bf6bb95c/lib/fluent/plugin_id.rb#L68-L78
            @stream_name = File.join(@stream_name, "worker#{fluentd_worker_id}")
          end
        end

        @path = File.join(@root_dir, @stream_name, @file_name)
        @dir_permission = (@dir_permission && @dir_permission.to_i(8)) || system_config.dir_permission || DIR_PERMISSION
      end

      def multi_workers_ready?
        true
      end

      def start
        FileUtils.mkdir_p(File.dirname(@path), mode: @dir_permission)
        @chunkio = ChunkIO.new(context_path: @root_dir, stream_name: @stream_name)

        super
      end

      def persistent?
        true
      end

      def resume
        stage, queue = load_existing_chunks([@path], chunkio: @chunkio)

        if @additional_resume_path
          c = ChunkIO.new(context_path: @root_dir, stream_name: @stream_name_original)
          s, q = load_existing_chunks(@additional_resume_path, chunkio: c)
          stage.merge!(s)
          queue.concat(q)
        end
        queue.sort_by!(&:modified_at)

        return stage, queue
      end

      private

      def load_existing_chunks(paths, chunkio:)
        stage = {}
        queue = []
        Dir.glob(paths) do |path|
          unless File.file?(path)
            next
          end

          chunk =
            begin
              Fluent::Plugin::Buffer::ChunkioChunk.new(new_metadata, path, :assume, chunk: chunkio)
            rescue Fluent::Plugin::Buffer::ChunkioChunk::FileChunkError => e
              handle_broken_files(path, e)
            end

          puts chunk

          case chunk.state
          when :staged
            stage[chunk.metadata] = chunk
          when :queued
            queue << chunk
          else
            # log?
            raise 'Invalid state', state: chunk.state
          end
        end

        return stage, queue
      end

      def generate_chunk(metadata)
        chunk = Fluent::Plugin::Buffer::ChunkioChunk.new(metadata, @path, :create, chunk: @chunkio, compress: @compress)
        log.debug('Created new chunk', chunk_id: dump_unique_id_hex(chunk.unique_id), metadata: metadata)
        chunk
      end

      def handle_broken_files(path, e)
        log.warn('found broken chunk file during resume. Delete corresponding files:', path: path, err_msg: e.message)
        # After support 'backup_dir' feature, these files are moved to backup_dir instead of unlink.
        File.unlink(path) rescue nil
      end
    end
  end
end
