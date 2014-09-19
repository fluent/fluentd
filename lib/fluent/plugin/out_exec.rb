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

require 'fluent/plugin/exec_util'

module Fluent
  class ExecOutput < TimeSlicedOutput
    Plugin.register_output('exec', self)

    def initialize
      super
      require 'tempfile'
      @localtime = false
    end

    config_param :command, :string
    config_param :keys, :default => [] do |val|
      val.split(',')
    end
    config_param :tag_key, :string, :default => nil
    config_param :time_key, :string, :default => nil
    config_param :time_format, :string, :default => nil
    config_param :format, :default => :tsv do |val|
      f = ExecUtil::SUPPORTED_FORMAT[val]
      raise ConfigError, "Unsupported format '#{val}'" unless f
      f
    end

    def configure(conf)
      super

      @formatter = case @format
                   when :tsv
                     if @keys.empty?
                       raise ConfigError, "keys option is required on exec output for tsv format"
                     end
                     ExecUtil::TSVFormatter.new(@keys)
                   when :json
                     ExecUtil::JSONFormatter.new
                   when :msgpack
                     ExecUtil::MessagePackFormatter.new
                   end

      if @time_key
        if @time_format
          tf = TimeFormatter.new(@time_format, @localtime, @timezone)
          @time_format_proc = tf.method(:format)
        else
          @time_format_proc = Proc.new { |time| time.to_s }
        end
      end
    end

    def format(tag, time, record)
      out = ''
      if @time_key
        record[@time_key] = @time_format_proc.call(time)
      end
      if @tag_key
        record[@tag_key] = tag
      end
      @formatter.call(record, out)
      out
    end

    def write(chunk)
      if chunk.respond_to?(:path)
        prog = "#{@command} #{chunk.path}"
      else
        tmpfile = Tempfile.new("fluent-plugin-exec-")
        chunk.write_to(tmpfile)
        tmpfile.close
        prog = "#{@command} #{tmpfile.path}"
      end

      system(prog)
      ecode = $?.to_i
      tmpfile.delete if tmpfile

      if ecode != 0
        raise "command returns #{ecode}: #{prog}"
      end
    end
  end
end
