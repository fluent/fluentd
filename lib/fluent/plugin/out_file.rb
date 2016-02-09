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

module Fluent
  class FileOutput < TimeSlicedOutput
    Plugin.register_output('file', self)

    SUPPORTED_COMPRESS = {
      'gz' => :gz,
      'gzip' => :gz,
    }

    desc "The Path of the file."
    config_param :path, :string
    desc "The format of the file content. The default is out_file."
    config_param :format, :string, default: 'out_file'
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

      @buffer.symlink_path = @symlink_path if @symlink_path
    end

    def format(tag, time, record)
      @formatter.format(tag, time, record)
    end

    def write(chunk)
      path = generate_path(chunk.key)
      FileUtils.mkdir_p File.dirname(path), mode: DEFAULT_DIR_PERMISSION

      case @compress
      when nil
        File.open(path, "a", DEFAULT_FILE_PERMISSION) {|f|
          chunk.write_to(f)
        }
      when :gz
        File.open(path, "a", DEFAULT_FILE_PERMISSION) {|f|
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
