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

require 'tempfile'

require 'fluent/plugin/output'
require 'fluent/config/error'
require 'fluent/plugin/exec_util'
require 'fluent/mixin' # for TimeFormatter

module Fluent::Plugin
  class ExecOutput < Output
    Fluent::Plugin.register_output('exec', self)

    helpers :compat_parameters

    desc 'The command (program) to execute. The exec plugin passes the path of a TSV file as the last argumen'
    config_param :command, :string
    desc 'Specify the comma-separated keys when using the tsv format.'
    config_param :keys, default: [] do |val|
      val.split(',')
    end
    desc 'The name of the key to use as the event tag. This replaces the value in the event record.'
    config_param :tag_key, :string, default: nil
    desc 'The name of the key to use as the event time. This replaces the the value in the event record.'
    config_param :time_key, :string, default: nil
    desc 'The format for event time used when the time_key parameter is specified. The default is UNIX time (integer).'
    config_param :time_format, :string, default: nil
    desc "The format used to map the incoming events to the program input. (#{Fluent::ExecUtil::SUPPORTED_FORMAT.keys.join(',')})"
    config_param :format, default: :tsv, skip_accessor: true do |val|
      f = Fluent::ExecUtil::SUPPORTED_FORMAT[val]
      raise Fluent::ConfigError, "Unsupported format '#{val}'" unless f
      f
    end
    config_param :localtime, :bool, default: false
    config_param :timezone, :string, default: nil

    def compat_parameters_default_chunk_key
      'time'
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer, default_chunk_key: 'time')

      super

      @formatter = case @format
                   when :tsv
                     if @keys.empty?
                       raise Fluent::ConfigError, "keys option is required on exec output for tsv format"
                     end
                     Fluent::ExecUtil::TSVFormatter.new(@keys)
                   when :json
                     Fluent::ExecUtil::JSONFormatter.new
                   when :msgpack
                     Fluent::ExecUtil::MessagePackFormatter.new
                   end

      if @time_key
        if @time_format
          tf = Fluent::TimeFormatter.new(@time_format, @localtime, @timezone)
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
        tmpfile.binmode
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
