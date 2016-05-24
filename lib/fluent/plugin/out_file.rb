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
require 'zlib'

require 'fluent/output'
require 'fluent/config/error'
require 'fluent/system_config'

module Fluent
  class FileOutput < TimeSlicedOutput
    include SystemConfig::Mixin

    Plugin.register_output('file', self)

    SUPPORTED_COMPRESS = {
      'gz' => :gz,
      'gzip' => :gz,
    }

    FILE_PERMISSION = 0644
    DIR_PERMISSION = 0755

    desc "The Path of the file."
    config_param :path, :string
    desc "The format of the file content. The default is out_file."
    config_param :format, :string, default: 'out_file', skip_accessor: true
    desc "The flushed chunk is appended to existence file or not."
    config_param :append, :bool, default: false
    desc "Compress flushed file."
    config_param :compress, default: nil do |val|
      c = SUPPORTED_COMPRESS[val]
      unless c
        raise ConfigError, "Unsupported compression algorithm '#{val}'"
      end
      c
    end
    desc "Create symlink to temporary buffered file when buffer_type is file."
    config_param :symlink_path, :string, default: nil

    module SymlinkBufferMixin
      def symlink_path=(path)
        @_symlink_path = path
      end

      def generate_chunk(metadata)
        chunk = super
        latest_chunk = metadata_list.sort_by(&:timekey).last
        if chunk.metadata == latest_chunk
          FileUtils.ln_sf(chunk.path, @_symlink_path)
        end
        chunk
      end
    end

    def initialize
      require 'zlib'
      require 'time'
      require 'fluent/plugin/file_util'
      super
    end

    def configure(conf)
      if path = conf['path']
        @path = path
      end
      unless @path
        raise ConfigError, "'path' parameter is required on file output"
      end

      if pos = @path.index('*')
        @path_prefix = @path[0,pos]
        @path_suffix = @path[pos+1..-1]
        conf['buffer_path'] ||= "#{@path}"
      else
        @path_prefix = @path+"."
        @path_suffix = ".log"
        conf['buffer_path'] ||= "#{@path}.*"
      end

      test_path = generate_path(Time.now.strftime(@time_slice_format))
      unless ::Fluent::FileUtil.writable_p?(test_path)
        raise ConfigError, "out_file: `#{test_path}` is not writable"
      end

      super

      @formatter = Plugin.new_formatter(@format)
      @formatter.configure(conf)

      if @symlink_path && @buffer.respond_to?(:path)
        (class << @buffer; self; end).module_eval do
          prepend SymlinkBufferMixin
        end
        @buffer.symlink_path = @symlink_path
      end

      @dir_perm = system_config.dir_permission || DIR_PERMISSION
      @file_perm = system_config.file_permission || FILE_PERMISSION
    end

    def format(tag, time, record)
      @formatter.format(tag, time, record)
    end

    def write(chunk)
      path = generate_path(chunk.key)
      FileUtils.mkdir_p File.dirname(path), mode: @dir_perm

      case @compress
      when nil
        File.open(path, "ab", @file_perm) {|f|
          chunk.write_to(f)
        }
      when :gz
        File.open(path, "ab", @file_perm) {|f|
          gz = Zlib::GzipWriter.new(f)
          chunk.write_to(gz)
          gz.close
        }
      end

      return path  # for test
    end

    def secondary_init(primary)
      # don't warn even if primary.class is not FileOutput
    end

    private

    def suffix
      case @compress
      when nil
        ''
      when :gz
        ".gz"
      end
    end

    def generate_path(time_string)
      if @append
        "#{@path_prefix}#{time_string}#{@path_suffix}#{suffix}"
      else
        path = nil
        i = 0
        begin
          path = "#{@path_prefix}#{time_string}_#{i}#{@path_suffix}#{suffix}"
          i += 1
        end while File.exist?(path)
        path
      end
    end
  end
end
