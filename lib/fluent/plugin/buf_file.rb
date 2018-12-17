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
require 'fluent/plugin/buffer/file_chunk'
require 'fluent/system_config'

module Fluent
  module Plugin
    class FileBuffer < Fluent::Plugin::Buffer
      Plugin.register_buffer('file', self)

      include SystemConfig::Mixin

      DEFAULT_CHUNK_LIMIT_SIZE = 256 * 1024 * 1024        # 256MB
      DEFAULT_TOTAL_LIMIT_SIZE =  64 * 1024 * 1024 * 1024 #  64GB, same with v0.12 (TimeSlicedOutput + buf_file)

      DIR_PERMISSION = 0755

      desc 'The path where buffer chunks are stored.'
      config_param :path, :string, default: nil

      config_set_default :chunk_limit_size, DEFAULT_CHUNK_LIMIT_SIZE
      config_set_default :total_limit_size, DEFAULT_TOTAL_LIMIT_SIZE

      config_param :file_permission, :string, default: nil # '0644'
      config_param :dir_permission,  :string, default: nil # '0755'

      @@buffer_paths = {}

      def initialize
        super
        @symlink_path = nil
        @multi_workers_available = false
        @additional_resume_path = nil
      end

      def configure(conf)
        super

        multi_workers_configured = owner.system_config.workers > 1 ? true : false

        using_plugin_root_dir = false
        unless @path
          if root_dir = owner.plugin_root_dir
            @path = File.join(root_dir, 'buffer')
            using_plugin_root_dir = true # plugin_root_dir path contains worker id
          else
            raise Fluent::ConfigError, "buffer path is not configured. specify 'path' in <buffer>"
          end
        end

        type_of_owner = Plugin.lookup_type_from_class(@_owner.class)
        if @@buffer_paths.has_key?(@path) && !called_in_test?
          type_using_this_path = @@buffer_paths[@path]
          raise ConfigError, "Other '#{type_using_this_path}' plugin already use same buffer path: type = #{type_of_owner}, buffer path = #{@path}"
        end

        @@buffer_paths[@path] = type_of_owner

        specified_directory_exists = File.exist?(@path) && File.directory?(@path)
        unexisting_path_for_directory = !File.exist?(@path) && !@path.include?('.*')

        if specified_directory_exists || unexisting_path_for_directory # directory
          if using_plugin_root_dir || !multi_workers_configured
            @path = File.join(@path, 'buffer.*.log')
          else
            @path = File.join(@path, "worker#{fluentd_worker_id}", 'buffer.*.log')
            if fluentd_worker_id == 0
              # worker 0 always checks unflushed buffer chunks to be resumed (might be created while non-multi-worker configuration)
              @additional_resume_path = File.join(File.expand_path("../../", @path), 'buffer.*.log')
            end
          end
          @multi_workers_available = true
        else # specified path is file path
          if File.basename(@path).include?('.*.')
            # valid file path
          elsif File.basename(@path).end_with?('.*')
            @path = @path + '.log'
          else
            # existing file will be ignored
            @path = @path + '.*.log'
          end
          @multi_workers_available = false
        end

        if @dir_permission
          @dir_permission = @dir_permission.to_i(8) if @dir_permission.is_a?(String)
        else
          @dir_permission = system_config.dir_permission || DIR_PERMISSION
        end
      end

      # This method is called only when multi worker is configured
      def multi_workers_ready?
        unless @multi_workers_available
          log.error "file buffer with multi workers should be configured to use directory 'path', or system root_dir and plugin id"
        end
        @multi_workers_available
      end

      def start
        FileUtils.mkdir_p File.dirname(@path), mode: @dir_permission

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
        Dir.glob(patterns) do |path|
          next unless File.file?(path)

          m = new_metadata() # this metadata will be overwritten by resuming .meta file content
                             # so it should not added into @metadata_list for now
          mode = Fluent::Plugin::Buffer::FileChunk.assume_chunk_state(path)
          if mode == :unknown
            log.debug "unknown state chunk found", path: path
            next
          end

          begin
            chunk = Fluent::Plugin::Buffer::FileChunk.new(m, path, mode) # file chunk resumes contents of metadata
          rescue Fluent::Plugin::Buffer::FileChunk::FileChunkError => e
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

        queue.sort_by!{ |chunk| chunk.modified_at }

        return stage, queue
      end

      def generate_chunk(metadata)
        # FileChunk generates real path with unique_id
        if @file_permission
          chunk = Fluent::Plugin::Buffer::FileChunk.new(metadata, @path, :create, perm: @file_permission, compress: @compress)
        else
          chunk = Fluent::Plugin::Buffer::FileChunk.new(metadata, @path, :create, compress: @compress)
        end

        log.debug "Created new chunk", chunk_id: dump_unique_id_hex(chunk.unique_id), metadata: metadata

        return chunk
      end

      def handle_broken_files(path, mode, e)
        log.error "found broken chunk file during resume. Deleted corresponding files:", :path => path, :mode => mode, :err_msg => e.message
        # After support 'backup_dir' feature, these files are moved to backup_dir instead of unlink.
        File.unlink(path, path + '.meta') rescue nil
      end
    end
  end
end
