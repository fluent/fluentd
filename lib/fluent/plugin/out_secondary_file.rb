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

require "fileutils"
require "fluent/plugin/file_util"
require "fluent/plugin/output"
require "fluent/config/error"

module Fluent::Plugin
  class SecondaryFileOutput < Output
    Fluent::Plugin.register_output("secondary_file", self)

    SUPPORTED_COMPRESS = {
      "gz" => :gz,
      "gzip" => :gz,
    }

    FILE_PERMISSION = 0644
    DIR_PERMISSION = 0755

    desc "The Path of the file."
    config_param :path, :string
    desc "The flushed chunk is appended to existence file or not."
    config_param :append, :bool, default: false
    config_param :compress, default: nil do |val|
      SUPPORTED_COMPRESS[val].tap do |e|
        raise ConfigError, "Unsupported compression algorithm '#{val}'" unless e
      end
    end

    def initialize
      super
    end

    def configure(conf)
      unless @as_secondary
        raise Fluent::ConfigError, "This plugin can only be used in the <secondary> section"
      end

      unless @path = conf["path"]
        raise Fluent::ConfigError, "'path' parameter is required on secondary file output"
      end

      if pos = @path.index('*')
        @path_prefix = @path[0,pos]
        @path_suffix = @path[pos+1..-1]
      else
        @path_prefix = @path + "."
        @path_suffix = ".log"
      end

      test_path = generate_path(Time.now.strftime("%Y%m%d"))
      unless Fluent::FileUtil.writable_p?(test_path)
        raise Fluent::ConfigError, "out_file: `#{test_path}` is not writable"
      end

      @dir_perm = system_config.dir_permission || DIR_PERMISSION
      @file_perm = system_config.file_permission || FILE_PERMISSION

      # @tempalte = genereate_tempalte fix
      super
    end

    def write(chunk)
      id = extract_placeholders('', chunk)
      path = generate_path(id)
      FileUtils.mkdir_p File.dirname(path), mode: @dir_perm

      case @compress
      when nil
        File.open(path, "ab", @file_perm) {|f|
          f.flock(File::LOCK_EX)
          chunk.write_to(f)
        }
      when :gz
        File.open(path, "ab", @file_perm) {|f|
          f.flock(File::LOCK_EX)
          gz = Zlib::GzipWriter.new(f)
          chunk.write_to(gz)
          gz.close
        }
      end

      path
    end

    def extract_placeholders(str, chunk)
      if chunk.metadata.nil?
        chunk.chunk_id
      else
        super(str, chunk.metadata)
      end
    end

    private

    def genereate_tempalte
      rvalue = ""
      if @chunk_key_tag
        rvalue += "{tag}_"
      end

      if @chunk_key_time
        rvalue += "%Y%m%d_"
      end

      @chunk_keys.each do |v|
        rvalue += "${#{v}}_"
      end

      if rvalue.end_with?("_")
        rvalue = rvalue[0..-2]
      end

      rvalue
    end

    def suffix
      case @compress
      when nil
        ""
      when :gz
        ".gz"
      end
    end

    def generate_path(id)
      if @append
        "#{@path_prefix}#{id}#{@path_suffix}#{suffix}"
      else
        i = 0
        loop do
          path = "#{@path_prefix}#{id}_#{i}#{@path_suffix}#{suffix}"
          return path unless File.exist?(path)
          i += 1;
        end
      end
    end
  end
end
