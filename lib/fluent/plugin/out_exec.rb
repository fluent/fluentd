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

module Fluent::Plugin
  class ExecOutput < Output
    Fluent::Plugin.register_output('exec', self)

    helpers :inject, :formatter, :compat_parameters

    desc 'The command (program) to execute. The exec plugin passes the path of a TSV file as the last argumen'
    config_param :command, :string

    config_section :format do
      config_set_default :@type, 'tsv'
    end

    config_section :inject do
      config_set_default :time_type, :string
      config_set_default :localtime, false
    end

    attr_reader :formatter # for tests

    def configure(conf)
      compat_parameters_convert(conf, :inject, :formatter, :buffer, default_chunk_key: 'time')
      super
      @formatter = formatter_create
    end

    def format(tag, time, record)
      record = inject_values_to_record(tag, time, record)
      if @formatter.formatter_type == :text_per_line
        @formatter.format(tag, time, record).chomp + "\n"
      else
        @formatter.format(tag, time, record)
      end
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
