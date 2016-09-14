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
require 'time'

require 'fluent/plugin/output'
require 'fluent/plugin/file_util'
require 'fluent/config/error'

module Fluent::Plugin
  class FileOutput < Output
    Fluent::Plugin.register_output('file', self)

    helpers :formatter, :inject, :compat_parameters

    SUPPORTED_COMPRESS = {
      'gz' => :gz,
      'gzip' => :gz,
    }

    FILE_PERMISSION = 0644
    DIR_PERMISSION = 0755
    DEFAULT_FORMAT_TYPE = "out_file"
    DEFAULT_BUFFER_TYPE = "file"

    desc "The Path of the file."
    config_param :path, :string
    desc "The flushed chunk is appended to existence file or not."
    config_param :append, :bool, default: false
    desc "Compress flushed file."
    config_param :compress, default: nil do |val|
      c = SUPPORTED_COMPRESS[val]
      unless c
        raise Fluent::ConfigError, "Unsupported compression algorithm '#{val}'"
      end
      c
    end
    desc "Create symlink to temporary buffered file when buffer_type is file."
    config_param :symlink_path, :string, default: nil

    config_section :buffer do
      config_set_default :@type, DEFAULT_BUFFER_TYPE
    end
    config_section :format do
      config_set_default :@type, DEFAULT_FORMAT_TYPE
    end

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

    # for test
    attr_reader :_paths

    def initialize
      super
      @_paths = []
    end

    def configure(conf)
      if path = conf['path']
        @path = path
      end
      unless @path
        raise Fluent::ConfigError, "'path' parameter is required on file output"
      end

      conf["buffer_path"] ||= @path # For backward compatibility

      compat_parameters_convert(conf, :formatter, :buffer, :inject, default_chunk_key: "time")

      if pos = @path.index('*')
        @path_prefix = @path[0,pos]
        @path_suffix = @path[pos+1..-1]
        conf['buffer_path'] ||= "#{@path}"
      else
        @path_prefix = @path+"."
        @path_suffix = ".log"
        conf['buffer_path'] ||= "#{@path}.*"
      end

      super

      @formatter = formatter_create(conf: @config.elements('format').first, default_type: DEFAULT_FORMAT_TYPE)

      if @symlink_path && @buffer.respond_to?(:path)
        @buffer.extend SymlinkBufferMixin
        @buffer.symlink_path = @symlink_path
      end

      @dir_perm = system_config.dir_permission || DIR_PERMISSION
      @file_perm = system_config.file_permission || FILE_PERMISSION

      test_path = generate_path(metadata("test.path", Fluent::EventTime.now, {}))
      unless ::Fluent::FileUtil.writable_p?(test_path)
        raise ::Fluent::ConfigError, "out_file: `#{test_path}` is not writable"
      end
    end

    def format(tag, time, record)
      r = inject_values_to_record(tag, time, record)
      @formatter.format(tag, time, r)
    end

    def write(chunk)
      path = generate_path(chunk.metadata)
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

      @_paths << path  # for test
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

    def generate_path(metadata)
      time_string = extract_placeholders(extract_time_format, metadata)
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

    def extract_time_format
      if @config["time_slice_format"]
        # For backward compatibility
        @config["time_slice_format"]
      else
        case @buffer_config && @buffer_config.timekey
        when 1
          "%Y%m%d%H%M%S"
        when 60
          "%Y%m%d%H%M"
        when 3600
          "%Y%m%d%H"
        when 86400
          "%Y%m%d"
        else
          "%Y%m%d"
        end
      end
    end
  end
end
