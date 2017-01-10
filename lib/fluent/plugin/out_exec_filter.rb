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
require 'fluent/plugin/output'
require 'fluent/env'
require 'fluent/config/error'

require 'yajl'

module Fluent::Plugin
  class ExecFilterOutput < Output
    Fluent::Plugin.register_output('exec_filter', self)

    helpers :compat_parameters, :inject, :formatter, :parser, :extract, :child_process, :event_emitter

    desc 'The command (program) to execute.'
    config_param :command, :string

    config_param :remove_prefix, :string, default: nil, deprecated: "use @label instead for event routing"
    config_param :add_prefix, :string, default: nil, deprecated: "use @label instead for event routing"

    config_section :inject do
      config_set_default :time_type, :unixtime
    end

    config_section :format do
      config_set_default :@type, 'tsv'
      config_set_default :localtime, true
    end

    config_section :parse do
      config_set_default :@type, 'tsv'
      config_set_default :time_key, nil
      config_set_default :time_format, nil
      config_set_default :localtime, true
      config_set_default :estimate_current_event, false
    end

    config_section :extract do
      config_set_default :time_type, :float
    end

    config_section :buffer do
      config_set_default :flush_mode, :interval
      config_set_default :flush_interval, 1
    end

    config_param :tag, :string, default: nil

    config_param :tag_key, :string, default: nil, deprecated: "use 'tag_key' in <inject>/<extract> instead"
    config_param :time_key, :string, default: nil, deprecated: "use 'time_key' in <inject>/<extract> instead"
    config_param :time_format, :string, default: nil, deprecated: "use 'time_format' in <inject>/<extract> instead"

    desc 'The default block size to read if parser requires partial read.'
    config_param :read_block_size, :size, default: 10240 # 10k

    desc 'The number of spawned process for command.'
    config_param :num_children, :integer, default: 1

    desc 'Respawn command when command exit. ["none", "inf" or positive integer for times to respawn (defaut: none)]'
    # nil, 'none' or 0: no respawn, 'inf' or -1: infinite times, positive integer: try to respawn specified times only
    config_param :child_respawn, :string, default: nil

    # 0: output logs for all of messages to emit
    config_param :suppress_error_log_interval, :time, default: 0

    attr_reader :formatter, :parser # for tests

    KEYS_FOR_IN_AND_OUT = {
      'tag_key' => ['in_tag_key', 'out_tag_key'],
      'time_key' => ['in_time_key', 'out_time_key'],
      'time_format' => ['in_time_format', 'out_time_format'],
    }
    COMPAT_INJECT_PARAMS = {
      'in_tag_key' => 'tag_key',
      'in_time_key' => 'time_key',
      'in_time_format' => 'time_format',
    }
    COMPAT_FORMAT_PARAMS = {
      'in_format' => '@type',
      'in_keys' => 'keys',
    }
    COMPAT_PARSE_PARAMS = {
      'out_format' => '@type',
      'out_keys' => 'keys',
    }
    COMPAT_EXTRACT_PARAMS = {
      'out_tag_key' => 'tag_key',
      'out_time_key' => 'time_key',
      'out_time_format' => 'time_format',
    }

    def exec_filter_compat_parameters_copy_to_subsection!(conf, subsection_name, params)
      return unless conf.elements(subsection_name).empty?
      return unless params.keys.any?{|k| conf.has_key?(k) }
      hash = {}
      params.each_pair do |compat, current|
        hash[current] = conf[compat] if conf.has_key?(compat)
      end
      conf.elements << Fluent::Config::Element.new(subsection_name, '', hash, [])
    end

    def exec_filter_compat_parameters_convert!(conf)
      KEYS_FOR_IN_AND_OUT.each_pair do |inout, keys|
        if conf.has_key?(inout)
          keys.each do |k|
            conf[k] = conf[inout]
          end
        end
      end
      exec_filter_compat_parameters_copy_to_subsection!(conf, 'inject', COMPAT_INJECT_PARAMS)
      exec_filter_compat_parameters_copy_to_subsection!(conf, 'format', COMPAT_FORMAT_PARAMS)
      exec_filter_compat_parameters_copy_to_subsection!(conf, 'parse', COMPAT_PARSE_PARAMS)
      exec_filter_compat_parameters_copy_to_subsection!(conf, 'extract', COMPAT_EXTRACT_PARAMS)
    end

    def configure(conf)
      exec_filter_compat_parameters_convert!(conf)
      compat_parameters_convert(conf, :buffer)

      if inject_section = conf.elements('inject').first
        if inject_section.has_key?('time_format')
          inject_section['time_type'] ||= 'string'
        end
      end
      if extract_section = conf.elements('extract').first
        if extract_section.has_key?('time_format')
          extract_section['time_type'] ||= 'string'
        end
      end

      super

      if !@tag && (!@extract_config || !@extract_config.tag_key)
        raise Fluent::ConfigError, "'tag' or '<extract> tag_key </extract>' option is required on exec_filter output"
      end

      @formatter = formatter_create
      @parser = parser_create

      if @remove_prefix
        @removed_prefix_string = @remove_prefix + '.'
        @removed_length = @removed_prefix_string.length
      end
      if @add_prefix
        @added_prefix_string = @add_prefix + '.'
      end

      @respawns = if @child_respawn.nil? or @child_respawn == 'none' or @child_respawn == '0'
                    0
                  elsif @child_respawn == 'inf' or @child_respawn == '-1'
                    -1
                  elsif @child_respawn =~ /^\d+$/
                    @child_respawn.to_i
                  else
                    raise ConfigError, "child_respawn option argument invalid: none(or 0), inf(or -1) or positive number"
                  end

      @suppress_error_log_interval ||= 0
      @next_log_time = Time.now.to_i
    end

    def multi_workers_ready?
      true
    end

    ExecutedProcess = Struct.new(:mutex, :pid, :respawns, :readio, :writeio)

    def start
      super

      @children_mutex = Mutex.new
      @children = []
      @rr = 0

      exit_callback = ->(status){
        c = @children.select{|child| child.pid == status.pid }.first
        if c
          unless self.stopped?
            log.warn "child process exits with error code", code: status.to_i, status: status.exitstatus, signal: status.termsig
          end
          c.mutex.synchronize do
            (c.writeio && c.writeio.close) rescue nil
            (c.readio && c.readio.close) rescue nil
            c.pid = c.readio = c.writeio = nil
          end
        end
      }
      child_process_callback = ->(index, readio, writeio){
        pid = child_process_id
        c = @children[index]
        writeio.sync = true
        c.mutex.synchronize do
          c.pid = pid
          c.respawns = @respawns
          c.readio = readio
          c.writeio = writeio
        end

        run(readio)
      }
      execute_child_process = ->(index){
        child_process_execute("out_exec_filter_child#{index}".to_sym, @command, on_exit_callback: exit_callback) do |readio, writeio|
          child_process_callback.call(index, readio, writeio)
        end
      }

      @children_mutex.synchronize do
        @num_children.times do |i|
          @children << ExecutedProcess.new(Mutex.new, nil, 0, nil, nil)
          execute_child_process.call(i)
        end
      end

      if @respawns != 0
        thread_create(:out_exec_filter_respawn_monitor) do
          while thread_current_running?
            @children.each_with_index do |c, i|
              if c.mutex && c.mutex.synchronize{ c.pid.nil? && c.respawns != 0 }
                respawns = c.mutex.synchronize do
                  c.respawns -= 1 if c.respawns > 0
                  c.respawns
                end
                log.info "respawning child process", num: i, respawn_counter: respawns
                execute_child_process.call(i)
              end
            end
            sleep 0.2
          end
        end
      end
    end

    def terminate
      @children = []
      super
    end

    def tag_remove_prefix(tag)
      if @remove_prefix
        if (tag[0, @removed_length] == @removed_prefix_string and tag.length > @removed_length) or tag == @removed_prefix_string
          tag = tag[@removed_length..-1] || ''
        end
      end
      tag
    end

    NEWLINE = "\n"

    def format(tag, time, record)
      tag = tag_remove_prefix(tag)
      record = inject_values_to_record(tag, time, record)
      if @formatter.formatter_type == :text_per_line
        @formatter.format(tag, time, record).chomp + NEWLINE
      else
        @formatter.format(tag, time, record)
      end
    end

    def write(chunk)
      try_times = 0
      while true
        r = @rr = (@rr + 1) % @children.length
        if @children[r].pid && writeio = @children[r].writeio
          chunk.write_to(writeio)
          break
        end
        try_times += 1
        raise "no healthy child processes exist" if try_times >= @children.length
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
      tag = @added_prefix_string + tag if tag && @add_prefix
      tag ||= @tag
      time ||= extract_time_from_record(record) || Fluent::EventTime.now
      router.emit(tag, time, record)
    rescue => e
      if @suppress_error_log_interval == 0 || Time.now.to_i > @next_log_time
        log.error "exec_filter failed to emit", record: Yajl.dump(record), error: e
        log.error_backtrace e.backtrace
        @next_log_time = Time.now.to_i + @suppress_error_log_interval
      end
      router.emit_error_event(tag, time, record, e) if tag && time && record
    end
  end
end
