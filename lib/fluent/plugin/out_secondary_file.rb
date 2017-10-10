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
    PLACEHOLDER_REGEX = /\${(tag(\[\d+\])?|[\w.@-]+)}/

    desc "The directory path of the output file."
    config_param :directory, :string
    desc "The basename of the output file."
    config_param :basename, :string, default: "dump.bin"
    desc "The flushed chunk is appended to existence file or not."
    config_param :append, :bool, default: false
    config_param :compress, :enum, list: [:text, :gzip], default: :text

    def configure(conf)
      super

      unless @as_secondary
        raise Fluent::ConfigError, "This plugin can only be used in the <secondary> section"
      end

      if @basename.include?("/")
        raise Fluent::ConfigError, "basename should not include `/`"
      end

      @path_without_suffix = File.join(@directory, @basename)
      validate_compatible_with_primary_buffer!(@path_without_suffix)

      @suffix = case @compress
                when :text
                  ""
                when :gzip
                  ".gz"
                end

      test_path = @path_without_suffix
      unless Fluent::FileUtil.writable_p?(test_path)
        raise Fluent::ConfigError, "out_secondary_file: `#{@directory}` should be writable"
      end

      @dir_perm = system_config.dir_permission || DIR_PERMISSION
      @file_perm = system_config.file_permission || FILE_PERMISSION
    end

    def multi_workers_ready?
      ### TODO: add hack to synchronize for multi workers
      true
    end

    def write(chunk)
      path_without_suffix = extract_placeholders(@path_without_suffix, chunk)
      path = generate_path(path_without_suffix)
      FileUtils.mkdir_p File.dirname(path), mode: @dir_perm

      case @compress
      when :text
        File.open(path, "ab", @file_perm) {|f|
          f.flock(File::LOCK_EX)
          chunk.write_to(f)
        }
      when :gzip
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

    def validate_compatible_with_primary_buffer!(path_without_suffix)
      placeholders = path_without_suffix.scan(PLACEHOLDER_REGEX).flat_map(&:first) # to trim suffix [\d+]

      if !@chunk_key_time && has_time_format?(path_without_suffix)
        raise Fluent::ConfigError, "out_secondary_file: basename or directory has an incompatible placeholder, remove time formats, like `%Y%m%d`, from basename or directory"
      end

      if !@chunk_key_tag && (ph = placeholders.find { |placeholder| placeholder.match(/tag(\[\d+\])?/) })
        raise Fluent::ConfigError, "out_secondary_file: basename or directory has an incompatible placeholder #{ph}, remove tag placeholder, like `${tag}`, from basename or directory"
      end

      vars = placeholders.reject { |placeholder| placeholder.match(/tag(\[\d+\])?/) || (placeholder == 'chunk_id') }

      if ph = vars.find { |v| !@chunk_keys.include?(v) }
        raise Fluent::ConfigError, "out_secondary_file: basename or directory has an incompatible placeholder #{ph}, remove variable placeholder, like `${varname}`, from basename or directory"
      end
    end

    def has_time_format?(str)
      str != Time.now.strftime(str)
    end

    def generate_path(path_without_suffix)
      if @append
        "#{path_without_suffix}#{@suffix}"
      else
        i = 0
        loop do
          path = "#{path_without_suffix}.#{i}#{@suffix}"
          return path unless File.exist?(path)
          i += 1
        end
      end
    end
  end
end
