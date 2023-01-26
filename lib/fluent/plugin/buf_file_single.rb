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
require 'fluent/plugin/buffer/file_single_chunk'
require 'fluent/system_config'
require 'fluent/variable_store'

module Fluent
  module Plugin
    class FileSingleBuffer < Fluent::Plugin::Buffer
      Plugin.register_buffer('file_single', self)

      include SystemConfig::Mixin

      DEFAULT_CHUNK_LIMIT_SIZE = 256 * 1024 * 1024        # 256MB
      DEFAULT_TOTAL_LIMIT_SIZE =  64 * 1024 * 1024 * 1024 #  64GB

      PATH_SUFFIX = ".#{Fluent::Plugin::Buffer::FileSingleChunk::PATH_EXT}"

      desc 'The path where buffer chunks are stored.'
      config_param :path, :string, default: nil
      desc 'Calculate the number of record in chunk during resume'
      config_param :calc_num_records, :bool, default: true
      desc 'The format of chunk. This is used to calculate the number of record'
      config_param :chunk_format, :enum, list: [:msgpack, :text, :auto], default: :auto

      config_set_default :chunk_limit_size, DEFAULT_CHUNK_LIMIT_SIZE
      config_set_default :total_limit_size, DEFAULT_TOTAL_LIMIT_SIZE

      desc 'The permission of chunk file. If no specified, <system> setting or 0644 is used'
      config_param :file_permission, :string, default: nil
      desc 'The permission of chunk directory. If no specified, <system> setting or 0755 is used'
      config_param :dir_permission, :string, default: nil

      def initialize
        super

        @multi_workers_available = false
        @additional_resume_path = nil
        @variable_store = nil
      end

      def configure(conf)
        super

        @variable_store = Fluent::VariableStore.fetch_or_build(:buf_file_single)

        if @chunk_format == :auto
          @chunk_format = owner.formatted_to_msgpack_binary? ? :msgpack : :text
        end

        @key_in_path = nil
        if owner.chunk_keys.empty?
          log.debug "use event tag for buffer key"
        else
          if owner.chunk_key_tag
            raise Fluent::ConfigError, "chunk keys must be tag or one field"
          elsif owner.chunk_keys.size > 1
            raise Fluent::ConfigError, "2 or more chunk keys is not allowed"
          else
            @key_in_path = owner.chunk_keys.first.to_sym
          end
        end

        multi_workers_configured = owner.system_config.workers > 1
        using_plugin_root_dir = false
        unless @path
          if root_dir = owner.plugin_root_dir
            @path = File.join(root_dir, 'buffer')
            using_plugin_root_dir = true # plugin_root_dir path contains worker id
          else
            raise Fluent::ConfigError, "buffer path is not configured. specify 'path' in <buffer>"
          end
        end

        specified_directory_exists = File.exist?(@path) && File.directory?(@path)
        unexisting_path_for_directory = !File.exist?(@path) && !@path.include?('.*')

        if specified_directory_exists || unexisting_path_for_directory # directory
          if using_plugin_root_dir || !multi_workers_configured
            @path = File.join(@path, "fsb.*#{PATH_SUFFIX}")
          else
            @path = File.join(@path, "worker#{fluentd_worker_id}", "fsb.*#{PATH_SUFFIX}")
            if fluentd_worker_id == 0
              # worker 0 always checks unflushed buffer chunks to be resumed (might be created while non-multi-worker configuration)
              @additional_resume_path = File.join(File.expand_path("../../", @path), "fsb.*#{PATH_SUFFIX}")
            end
          end
          @multi_workers_available = true
        else # specified path is file path
          if File.basename(@path).include?('.*.')
            new_path = File.join(File.dirname(@path), "fsb.*#{PATH_SUFFIX}")
            log.warn "file_single doesn't allow user specified 'prefix.*.suffix' style path. Use '#{new_path}' for file instead: #{@path}"
            @path = new_path
          elsif File.basename(@path).end_with?('.*')
            @path = @path + PATH_SUFFIX
          else
            # existing file will be ignored
            @path = @path + ".*#{PATH_SUFFIX}"
          end
          @multi_workers_available = false
        end

        type_of_owner = Plugin.lookup_type_from_class(@_owner.class)
        if @variable_store.has_key?(@path) && !called_in_test?
          type_using_this_path = @variable_store[@path]
          raise Fluent::ConfigError, "Other '#{type_using_this_path}' plugin already uses same buffer path: type = #{type_of_owner}, buffer path = #{@path}"
        end

        @variable_store[@path] = type_of_owner
        @dir_permission = if @dir_permission
                            @dir_permission.to_i(8)
                          else
                            system_config.dir_permission || Fluent::DEFAULT_DIR_PERMISSION
                          end
      end

      # This method is called only when multi worker is configured
      def multi_workers_ready?
        unless @multi_workers_available
          log.error "file_single buffer with multi workers should be configured to use directory 'path', or system root_dir and plugin id"
        end
        @multi_workers_available
      end

      def start
        FileUtils.mkdir_p(File.dirname(@path), mode: @dir_permission)

        super
      end

      def stop
        if @variable_store
          @variable_store.delete(@path)
        end

        super
      end

      def persistent?
        true
      end

      def resume
        stage = {}
        queue = []

        patterns = [@path]
        patterns.unshift @additional_resume_path if @additional_resume_path
        Dir.glob(escaped_patterns(patterns)) do |path|
          next unless File.file?(path)

          log.debug { "restoring buffer file: path = #{path}" }

          m = new_metadata() # this metadata will be updated in FileSingleChunk.new
          mode = Fluent::Plugin::Buffer::FileSingleChunk.assume_chunk_state(path)
          if mode == :unknown
            log.debug "unknown state chunk found", path: path
            next
          end

          begin
            chunk = Fluent::Plugin::Buffer::FileSingleChunk.new(m, path, mode, @key_in_path, compress: @compress)
            chunk.restore_size(@chunk_format) if @calc_num_records
          rescue Fluent::Plugin::Buffer::FileSingleChunk::FileChunkError => e
            handle_broken_files(path, mode, e)
            next
          end

          case chunk.state
          when :staged
            stage[chunk.metadata] = chunk
          when :queued
            queue << chunk
          end
        end

        queue.sort_by!(&:modified_at)

        return stage, queue
      end

      def generate_chunk(metadata)
        # FileChunk generates real path with unique_id
        perm = @file_permission || system_config.file_permission
        chunk = Fluent::Plugin::Buffer::FileSingleChunk.new(metadata, @path, :create, @key_in_path, perm: perm, compress: @compress)

        log.debug "Created new chunk", chunk_id: dump_unique_id_hex(chunk.unique_id), metadata: metadata

        chunk
      end

      def handle_broken_files(path, mode, e)
        log.error "found broken chunk file during resume.", :path => path, :mode => mode, :err_msg => e.message
        unique_id, _ = Fluent::Plugin::Buffer::FileSingleChunk.unique_id_and_key_from_path(path)
        if @disable_chunk_backup
          log.warn "disable_chunk_backup is true. #{dump_unique_id_hex(unique_id)} chunk is thrown away"
          return
        end
        backup(unique_id) { |f|
          File.open(path, 'rb') { |chunk|
            chunk.set_encoding(Encoding::ASCII_8BIT)
            chunk.sync = true
            chunk.binmode
            IO.copy_stream(chunk, f)
          }
        }
      rescue => error
        log.error "backup failed. Delete corresponding files.", :err_msg => error.message
      ensure
        File.unlink(path) rescue nil
      end

      private

      def escaped_patterns(patterns)
        patterns.map { |pattern|
          # '{' '}' are special character in Dir.glob
          pattern.gsub(/[\{\}]/) { |c| "\\#{c}" }
        }
      end
    end
  end
end
