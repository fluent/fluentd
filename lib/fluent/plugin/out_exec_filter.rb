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

require 'yajl'

require 'fluent/output'
require 'fluent/env'
require 'fluent/time'
require 'fluent/timezone'
require 'fluent/plugin/exec_util'
require 'fluent/config/error'

module Fluent
  class ExecFilterOutput < BufferedOutput
    Plugin.register_output('exec_filter', self)

    def initialize
      super
      require 'fluent/timezone'
    end

    desc 'The command (program) to execute.'
    config_param :command, :string

    config_param :remove_prefix, :string, default: nil
    config_param :add_prefix, :string, default: nil

    desc "The format used to map the incoming event to the program input.(#{Fluent::ExecUtil::SUPPORTED_FORMAT.keys.join(',')})"
    config_param :in_format, default: :tsv do |val|
      f = Fluent::ExecUtil::SUPPORTED_FORMAT[val]
      raise ConfigError, "Unsupported in_format '#{val}'" unless f
      f
    end
    desc 'Specify comma-separated values for tsv format.'
    config_param :in_keys, default: [] do |val|
      val.split(',')
    end
    desc 'The name of the key to use as the event tag.'
    config_param :in_tag_key, default: nil
    desc 'The name of the key to use as the event time.'
    config_param :in_time_key, default: nil
    desc 'The format for event time used when the in_time_key parameter is specified.(Defauls is UNIX time)'
    config_param :in_time_format, default: nil

    desc "The format used to process the program output.(#{Fluent::ExecUtil::SUPPORTED_FORMAT.keys.join(',')})"
    config_param :out_format, default: :tsv do |val|
      f = Fluent::ExecUtil::SUPPORTED_FORMAT[val]
      raise ConfigError, "Unsupported out_format '#{val}'" unless f
      f
    end
    desc 'Specify comma-separated values for tsv format.'
    config_param :out_keys, default: [] do |val|  # for tsv format
      val.split(',')
    end
    desc 'The name of the key to use as the event tag.'
    config_param :out_tag_key, default: nil
    desc 'The name of the key to use as the event time.'
    config_param :out_time_key, default: nil
    desc 'The format for event time used when the in_time_key parameter is specified.(Defauls is UNIX time)'
    config_param :out_time_format, default: nil

    config_param :tag, :string, default: nil

    config_param :time_key, :string, default: nil
    config_param :time_format, :string, default: nil

    desc 'If true, use localtime with in_time_format.'
    config_param :localtime, :bool, default: true
    desc 'If true, use timezone with in_time_format.'
    config_param :timezone, :string, default: nil
    desc 'The number of spawned process for command.'
    config_param :num_children, :integer, default: 1

    desc 'Respawn command when command exit.'
    # nil, 'none' or 0: no respawn, 'inf' or -1: infinite times, positive integer: try to respawn specified times only
    config_param :child_respawn, :string, default: nil

    # 0: output logs for all of messages to emit
    config_param :suppress_error_log_interval, :time, default: 0

    config_set_default :flush_interval, 1

    def configure(conf)
      if tag_key = conf['tag_key']
        # TODO obsoleted?
        @in_tag_key = tag_key
        @out_tag_key = tag_key
      end

      if time_key = conf['time_key']
        # TODO obsoleted?
        @in_time_key = time_key
        @out_time_key = time_key
      end

      if time_format = conf['time_format']
        # TODO obsoleted?
        @in_time_format = time_format
        @out_time_format = time_format
      end

      super

      if conf['localtime']
        @localtime = true
      elsif conf['utc']
        @localtime = false
      end

      if conf['timezone']
        @timezone = conf['timezone']
        Fluent::Timezone.validate!(@timezone)
      end

      if !@tag && !@out_tag_key
        raise ConfigError, "'tag' or 'out_tag_key' option is required on exec_filter output"
      end

      if @in_time_key
        if f = @in_time_format
          tf = TimeFormatter.new(f, @localtime, @timezone)
          @time_format_proc = tf.method(:format)
        else
          @time_format_proc = Proc.new {|time| time.to_s }
        end
      elsif @in_time_format
        log.warn "in_time_format effects nothing when in_time_key is not specified: #{conf}"
      end

      if @out_time_key
        if f = @out_time_format
          @time_parse_proc =
            begin
              strptime = Strptime.new(f)
              Proc.new { |str| Fluent::EventTime.from_time(strptime.exec(str)) }
            rescue
              Proc.new {|str| Fluent::EventTime.from_time(Time.strptime(str, f)) }
            end
        else
          @time_parse_proc = Proc.new {|str| Fluent::EventTime.from_time(Time.at(str.to_f)) }
        end
      elsif @out_time_format
        log.warn "out_time_format effects nothing when out_time_key is not specified: #{conf}"
      end

      if @remove_prefix
        @removed_prefix_string = @remove_prefix + '.'
        @removed_length = @removed_prefix_string.length
      end
      if @add_prefix
        @added_prefix_string = @add_prefix + '.'
      end

      case @in_format
      when :tsv
        if @in_keys.empty?
          raise ConfigError, "in_keys option is required on exec_filter output for tsv in_format"
        end
        @formatter = Fluent::ExecUtil::TSVFormatter.new(@in_keys)
      when :json
        @formatter = Fluent::ExecUtil::JSONFormatter.new
      when :msgpack
        @formatter = Fluent::ExecUtil::MessagePackFormatter.new
      end

      case @out_format
      when :tsv
        if @out_keys.empty?
          raise ConfigError, "out_keys option is required on exec_filter output for tsv in_format"
        end
        @parser = Fluent::ExecUtil::TSVParser.new(@out_keys, method(:on_message))
      when :json
        @parser = Fluent::ExecUtil::JSONParser.new(method(:on_message))
      when :msgpack
        @parser = Fluent::ExecUtil::MessagePackParser.new(method(:on_message))
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

    def start
      super

      @children = []
      @rr = 0
      begin
        @num_children.times do
          c = ChildProcess.new(@parser, @respawns, log)
          c.start(@command)
          @children << c
        end
      rescue
        shutdown
        raise
      end
    end

    def before_shutdown
      log.debug "out_exec_filter#before_shutdown called"
      @children.each {|c|
        c.finished = true
      }
      sleep 0.5  # TODO wait time before killing child process

      super
    end

    def shutdown
      @children.reject! {|c|
        c.shutdown
        true
      }

      super
    end

    def format_stream(tag, es)
      if @remove_prefix
        if (tag[0, @removed_length] == @removed_prefix_string and tag.length > @removed_length) or tag == @removed_prefix
          tag = tag[@removed_length..-1] || ''
        end
      end

      out = ''

      es.each {|time,record|
        if @in_time_key
          record[@in_time_key] = @time_format_proc.call(time)
        end
        if @in_tag_key
          record[@in_tag_key] = tag
        end
        @formatter.call(record, out)
      }

      out
    end

    def write(chunk)
      r = @rr = (@rr + 1) % @children.length
      @children[r].write chunk
    end

    class ChildProcess
      attr_accessor :finished

      def initialize(parser, respawns=0, log = $log)
        @pid = nil
        @thread = nil
        @parser = parser
        @respawns = respawns
        @mutex = Mutex.new
        @finished = nil
        @log = log
      end

      def start(command)
        @command = command
        @mutex.synchronize do
          @io = IO.popen(command, "r+")
          @pid = @io.pid
          @io.sync = true
          @thread = Thread.new(&method(:run))
        end
        @finished = false
      end

      def kill_child(join_wait)
        begin
          signal = Fluent.windows? ? :KILL : :TERM
          Process.kill(signal, @pid)
        rescue #Errno::ECHILD, Errno::ESRCH, Errno::EPERM
          # Errno::ESRCH 'No such process', ignore
          # child process killed by signal chained from fluentd process
        end
        if @thread.join(join_wait)
          # @thread successfully shutdown
          return
        end
        begin
          Process.kill(:KILL, @pid)
        rescue #Errno::ECHILD, Errno::ESRCH, Errno::EPERM
          # ignore if successfully killed by :TERM
        end
        @thread.join
      end

      def shutdown
        @finished = true
        @mutex.synchronize do
          kill_child(60) # TODO wait time
        end
      end

      def write(chunk)
        begin
          chunk.write_to(@io)
        rescue Errno::EPIPE => e
          # Broken pipe (child process unexpectedly exited)
          @log.warn "exec_filter Broken pipe, child process maybe exited.", command: @command
          if try_respawn
            retry # retry chunk#write_to with child respawned
          else
            raise e # to retry #write with other ChildProcess instance (when num_children > 1)
          end
        end
      end

      def try_respawn
        return false if @respawns == 0
        @mutex.synchronize do
          return false if @respawns == 0

          kill_child(5) # TODO wait time

          @io = IO.popen(@command, "r+")
          @pid = @io.pid
          @io.sync = true
          @thread = Thread.new(&method(:run))

          @respawns -= 1 if @respawns > 0
        end
        @log.warn "exec_filter child process successfully respawned.", command: @command, respawns: @respawns
        true
      end

      def run
        @parser.call(@io)
      rescue
        @log.error "exec_filter thread unexpectedly failed with an error.", command: @command, error: $!.to_s
        @log.warn_backtrace $!.backtrace
      ensure
        _pid, stat = Process.waitpid2(@pid)
        unless @finished
          @log.error "exec_filter process unexpectedly exited.", command: @command, ecode: stat.to_i
          unless @respawns == 0
            @log.warn "exec_filter child process will respawn for next input data (respawns #{@respawns})."
          end
        end
      end
    end

    def on_message(record)
      if val = record.delete(@out_time_key)
        time = @time_parse_proc.call(val)
      else
        time = Engine.now
      end

      if val = record.delete(@out_tag_key)
        tag = if @add_prefix
                @added_prefix_string + val
              else
                val
              end
      else
        tag = @tag
      end

      router.emit(tag, time, record)
    rescue
      if @suppress_error_log_interval == 0 || Time.now.to_i > @next_log_time
        log.error "exec_filter failed to emit", error: $!, record: Yajl.dump(record)
        log.warn_backtrace $!.backtrace
        @next_log_time = Time.now.to_i + @suppress_error_log_interval
      end
    end
  end
end
