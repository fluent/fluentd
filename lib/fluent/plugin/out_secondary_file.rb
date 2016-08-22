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

    FILE_PERMISSION = 0644
    DIR_PERMISSION = 0755

    desc "The Path of the file."
    config_param :path, :string
    desc "The flushed chunk is appended to existence file or not."
    config_param :append, :bool, default: false
    config_param :compress, :enum, list: [:text, :gz, :gzip], default: :text

    def configure(conf)
      super

      unless @as_secondary
        raise Fluent::ConfigError, "This plugin can only be used in the <secondary> section"
      end

      @placeholders = @path.scan(/\${([\w.@-]+(\[\d+\])?)}/).flat_map(&:first) # to trim suffix [\d+]

      validate_path_is_comptible_with_primary_buffer!

      if !@placeholders.empty? || path_has_time_format?
        @path_prefix = ""
        @path_suffix = ".log"
      else
        if pos = @path.index('*')
          @path_prefix = @path[0, pos]
          @path_suffix = @path[pos+1..-1]
        else
          @path_prefix = @path + "."
          @path_suffix = ".log"
        end
      end

      test_path = generate_path(Time.now.strftime("%Y%m%d"))
      unless Fluent::FileUtil.writable_p?(test_path)
        raise Fluent::ConfigError, "out_secondary_file: `#{test_path}` is not writable"
      end

      @dir_perm = system_config.dir_permission || DIR_PERMISSION
      @file_perm = system_config.file_permission || FILE_PERMISSION
    end

    def write(chunk)
      id = generate_id(chunk)
      path = generate_path(id)
      FileUtils.mkdir_p File.dirname(path), mode: @dir_perm

      case @compress
      when :text
        File.open(path, "ab", @file_perm) {|f|
          f.flock(File::LOCK_EX)
          chunk.write_to(f)
        }
      when :gz, :gzip
        File.open(path, "ab", @file_perm) {|f|
          f.flock(File::LOCK_EX)
          gz = Zlib::GzipWriter.new(f)
          chunk.write_to(gz)
          gz.close
        }
      end

      path
    end

    private

    def generate_id(chunk)
      if chunk.metadata.empty?
        dump_unique_id_hex(chunk.unique_id)
      else
        extract_placeholders(@path, chunk.metadata)
      end
    end

    def validate_path_is_comptible_with_primary_buffer!
      if !@chunk_key_time && path_has_time_format?
        raise Fluent::ConfigError, "out_secondary_file: File path has an incompatible placeholder, add time formats, like `%Y%m%d`, to `path`"
      end

      if !@chunk_key_tag && (ph = @placeholders.find { |p| p.match(/tag(\[\d+\])?/) })
        raise Fluent::ConfigError, "out_secondary_file: File path has an incompatible placeholder #{ph}, add tag placeholder, like `${tag}`, to `path`"
      end

      if @chunk_keys.empty?
        vars = @placeholders.reject { |p| p.match(/tag(\[\d+\])?/) }

        if ph = vars.find { |v| !@chunk_keys.include?(v) }
          raise Fluent::ConfigError, "out_secondary_file: File path has an incompatible placeholder #{ph}, add variable placeholder, like `${varname}`, to `path`"
        end
      end
    end


    def path_has_time_format?
      @path != Time.now.strftime(@path)
    end

    def suffix
      case @compress
      when :text
        ""
      when :gz, :gzip
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
