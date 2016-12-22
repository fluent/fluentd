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

    helpers :inject, :formatter, :compat_parameters, :child_process

    desc 'The command (program) to execute. The exec plugin passes the path of a TSV file as the last argument'
    config_param :command, :string

    config_param :command_timeout, :time, default: 270 # 4min 30sec

    config_section :format do
      config_set_default :@type, 'tsv'
    end

    config_section :inject do
      config_set_default :time_type, :string
      config_set_default :localtime, false
    end

    config_section :buffer do
      config_set_default :delayed_commit_timeout, 300 # 5 min
    end

    attr_reader :formatter # for tests

    def configure(conf)
      compat_parameters_convert(conf, :inject, :formatter, :buffer, default_chunk_key: 'time')
      super
      @formatter = formatter_create
    end

    def multi_workers_ready?
      true
    end

    NEWLINE = "\n"

    def format(tag, time, record)
      record = inject_values_to_record(tag, time, record)
      if @formatter.formatter_type == :text_per_line
        @formatter.format(tag, time, record).chomp + NEWLINE
      else
        @formatter.format(tag, time, record)
      end
    end

    def try_write(chunk)
      tmpfile = nil
      prog = if chunk.respond_to?(:path)
               "#{@command} #{chunk.path}"
             else
               tmpfile = Tempfile.new("fluent-plugin-out-exec-")
               tmpfile.binmode
               chunk.write_to(tmpfile)
               tmpfile.close
               "#{@command} #{tmpfile.path}"
             end
      chunk_id = chunk.unique_id
      callback = ->(status){
        begin
          if tmpfile
            tmpfile.delete rescue nil
          end
          if status && status.success?
            commit_write(chunk_id)
          elsif status
            # #rollback_write will be done automatically if it isn't called at here.
            # But it's after command_timeout, and this timeout should be longer than users expectation.
            # So here, this plugin calls it explicitly.
            rollback_write(chunk_id)
            log.warn "command exits with error code", prog: prog, status: status.exitstatus, signal: status.termsig
          else
            rollback_write(chunk_id)
            log.warn "command unexpectedly exits without exit status", prog: prog
          end
        rescue => e
          log.error "unexpected error in child process callback", error: e
        end
      }
      child_process_execute(:out_exec_process, prog, stderr: :connect, immediate: true, parallel: true, mode: [], wait_timeout: @command_timeout, on_exit_callback: callback)
    end
  end
end
