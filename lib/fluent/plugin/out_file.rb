#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent
  class FileOutput < TimeSlicedOutput
    Plugin.register_output('file', self)

    SUPPORTED_COMPRESS = {
      'gz' => :gz,
      'gzip' => :gz,
    }

    config_param :path, :string
    config_param :format, :string, :default => 'out_file'
    config_param :append, :bool, :default => false
    config_param :compress, :default => nil do |val|
      c = SUPPORTED_COMPRESS[val]
      unless c
        raise ConfigError, "Unsupported compression algorithm '#{val}'"
      end
      c
    end
    config_param :symlink_path, :string, :default => nil

    def initialize
      require 'zlib'
      require 'time'
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

      super

      conf['format'] = @format
      @formatter = TextFormatter.create(conf)

      @buffer.symlink_path = @symlink_path if @symlink_path
    end

    def format(tag, time, record)
      @formatter.format(tag, time, record)
    end

    def write(chunk)
      path = generate_path(chunk)
      FileUtils.mkdir_p File.dirname(path)

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

    def generate_path(chunk)
      case @compress
      when nil
        suffix = ''
      when :gz
        suffix = ".gz"
      end

      if @append
        "#{@path_prefix}#{chunk.key}#{@path_suffix}#{suffix}"
      else
        path = nil
        i = 0
        begin
          path = "#{@path_prefix}#{chunk.key}_#{i}#{@path_suffix}#{suffix}"
          i += 1
        end while File.exist?(path)
        path
      end
    end
  end
end
