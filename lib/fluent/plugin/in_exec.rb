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

require 'fluent/plugin/input'
require 'yajl'

module Fluent::Plugin
  class ExecInput < Fluent::Plugin::Input
    Fluent::Plugin.register_input('exec', self)

    helpers :compat_parameters, :extract, :parser, :child_process

    desc 'The command (program) to execute.'
    config_param :command, :string

    config_section :parse do
      config_set_default :@type, 'tsv'
      config_set_default :time_type, :float
      config_set_default :time_key, nil
      config_set_default :estimate_current_event, false
    end

    config_section :extract do
      config_set_default :time_type, :float
    end

    desc 'Tag of the output events.'
    config_param :tag, :string, default: nil
    desc 'The interval time between periodic program runs.'
    config_param :run_interval, :time, default: nil
    desc 'The default block size to read if parser requires partial read.'
    config_param :read_block_size, :size, default: 10240 # 10k

    attr_reader :parser

    def configure(conf)
      compat_parameters_convert(conf, :extract, :parser)
      ['parse', 'extract'].each do |subsection_name|
        if subsection = conf.elements(subsection_name).first
          if subsection.has_key?('time_format')
            subsection['time_type'] ||= 'string'
          end
        end
      end

      super

      if !@tag && (!@extract_config || !@extract_config.tag_key)
        raise Fluent::ConfigError, "'tag' or 'tag_key' option is required on exec input"
      end
      @parser = parser_create
    end

    def multi_workers_ready?
      true
    end

    def start
      super

      if @run_interval
        child_process_execute(:exec_input, @command, interval: @run_interval, mode: [:read], &method(:run))
      else
        child_process_execute(:exec_input, @command, immediate: true, mode: [:read], &method(:run))
      end
    end

    def run(io)
      case
      when @parser.implement?(:parse_io)
        @parser.parse_io(io, &method(:on_record))
      when @parser.implement?(:parse_partial_data)
        until io.eof?
          @parser.parse_partial_data(io.readpartial(@read_block_size), &method(:on_record))
        end
      when @parser.parser_type == :text_per_line
        io.each_line do |line|
          @parser.parse(line.chomp, &method(:on_record))
        end
      else
        @parser.parse(io.read, &method(:on_record))
      end
    end

    def on_record(time, record)
      tag = extract_tag_from_record(record)
      tag ||= @tag
      time ||= extract_time_from_record(record) || Fluent::EventTime.now
      router.emit(tag, time, record)
    rescue => e
      log.error "exec failed to emit", tag: tag, record: Yajl.dump(record), error: e
      router.emit_error_event(tag, time, record, e) if tag && time && record
    end
  end
end
