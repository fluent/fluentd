#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent


class ExecFilterOutput < BufferedOutput
  Plugin.register_output('exec_filter', self)

  def initialize
    super
  end

  SUPPORTED_FORMAT = {
    'tsv' => :tsv,
    'json' => :json,
    'msgpack' => :msgpack,
  }

  config_param :command, :string

  config_param :remove_prefix, :string, :default => nil
  config_param :add_prefix, :string, :default => nil

  config_param :in_format, :default => :tsv do |val|
    f = SUPPORTED_FORMAT[val]
    raise ConfigError, "Unsupported in_format '#{val}'" unless f
    f
  end
  config_param :in_keys, :default => [] do |val|
    val.split(',')
  end
  config_param :in_tag_key, :default => nil
  config_param :in_time_key, :default => nil
  config_param :in_time_format, :default => nil

  config_param :out_format, :default => :tsv do |val|
    f = SUPPORTED_FORMAT[val]
    raise ConfigError, "Unsupported out_format '#{val}'" unless f
    f
  end
  config_param :out_keys, :default => [] do |val|  # for tsv format
    val.split(',')
  end
  config_param :out_tag_key, :default => nil
  config_param :out_time_key, :default => nil
  config_param :out_time_format, :default => nil

  config_param :tag, :string, :default => nil

  config_param :time_key, :string, :default => nil
  config_param :time_format, :string, :default => nil

  config_param :localtime, :bool, :default => true
  config_param :num_children, :integer, :default => 1

  # nil, 'none' or 0: no respawn, 'inf' or -1: infinite times, positive integer: try to respawn specified times only
  config_param :child_respawn, :string, :default => nil

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

    if localtime = conf['localtime']
      @localtime = true
    elsif utc = conf['utc']
      @localtime = false
    end

    if !@tag && !@out_tag_key
      raise ConfigError, "'tag' or 'out_tag_key' option is required on exec_filter output"
    end

    if @in_time_key
      if f = @in_time_format
        tf = TimeFormatter.new(f, @localtime)
        @time_format_proc = tf.method(:format)
      else
        @time_format_proc = Proc.new {|time| time.to_s }
      end
    elsif @in_time_format
      $log.warn "in_time_format effects nothing when in_time_key is not specified: #{conf}"
    end

    if @out_time_key
      if f = @out_time_format
        @time_parse_proc = Proc.new {|str| Time.strptime(str, f).to_i }
      else
        @time_parse_proc = Proc.new {|str| str.to_i }
      end
    elsif @out_time_format
      $log.warn "out_time_format effects nothing when out_time_key is not specified: #{conf}"
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
      @formatter = TSVFormatter.new(@in_keys)
    when :json
      @formatter = JSONFormatter.new
    when :msgpack
      @formatter = MessagePackFormatter.new
    end

    case @out_format
    when :tsv
      if @out_keys.empty?
        raise ConfigError, "out_keys option is required on exec_filter output for tsv in_format"
      end
      @parser = TSVParser.new(@out_keys, method(:on_message))
    when :json
      @parser = JSONParser.new(method(:on_message))
    when :msgpack
      @parser = MessagePackParser.new(method(:on_message))
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
  end

  def start
    super

    @children = []
    @rr = 0
    begin
      @num_children.times do
        c = ChildProcess.new(@parser, @respawns)
        c.start(@command)
        @children << c
      end
    rescue
      shutdown
      raise
    end
  end

  def before_shutdown
    super
    sleep 0.5  # TODO wait time before killing child process
  end

  def shutdown
    super

    @children.reject! {|c|
      c.shutdown
      true
    }
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
    def initialize(parser,respawns=0)
      @pid = nil
      @thread = nil
      @parser = parser
      @respawns = respawns
      @mutex = Mutex.new
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
        Process.kill(:TERM, @pid)
      rescue Errno::ESRCH
        # Errno::ESRCH 'No such process', ignore
        # child process killed by signal chained from fluentd process
      end
      if @thread.join(join_wait)
        # @thread successfully shutdown
        return
      end
      begin
        Process.kill(:KILL, @pid)
      rescue Errno::ESRCH
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
        $log.warn "exec_filter Broken pipe, child process maybe exited.", :command => @command
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
      $log.warn "exec_filter child process successfully respawned.", :command => @command, :respawns => @respawns
      true
    end

    def run
      @parser.call(@io)
    rescue
      $log.error "exec_filter thread unexpectedly failed with an error.", :command=>@command, :error=>$!.to_s
      $log.warn_backtrace $!.backtrace
    ensure
      pid, stat = Process.waitpid2(@pid)
      unless @finished
        $log.error "exec_filter process unexpectedly exited.", :command=>@command, :ecode=>stat.to_i
        unless @respawns == 0
          $log.warn "exec_filter child process will respawn for next input data (respawns #{@respawns})."
        end
      end
    end
  end

  class Formatter
  end

  class TSVFormatter < Formatter
    def initialize(in_keys)
      @in_keys = in_keys
      super()
    end

    def call(record, out)
      last = @in_keys.length-1
      for i in 0..last
        key = @in_keys[i]
        out << record[key].to_s
        out << "\t" if i != last
      end
      out << "\n"
    end
  end

  class JSONFormatter < Formatter
    def call(record, out)
      out << Yajl.dump(record) << "\n"
    end
  end

  class MessagePackFormatter < Formatter
    def call(record, out)
      record.to_msgpack(out)
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

    Engine.emit(tag, time, record)

  rescue
    $log.error "exec_filter failed to emit", :error=>$!.to_s, :record=>Yajl.dump(record)
    $log.warn_backtrace $!.backtrace
  end

  class Parser
    def initialize(on_message)
      @on_message = on_message
    end
  end

  class TSVParser < Parser
    def initialize(out_keys, on_message)
      @out_keys = out_keys
      super(on_message)
    end

    def call(io)
      io.each_line(&method(:each_line))
    end

    def each_line(line)
      line.chomp!
      vals = line.split("\t")

      record = Hash[@out_keys.zip(vals)]

      @on_message.call(record)
    end
  end

  class JSONParser < Parser
    def call(io)
      y = Yajl::Parser.new
      y.on_parse_complete = @on_message
      y.parse(io)
    end
  end

  class MessagePackParser < Parser
    def call(io)
      @u = MessagePack::Unpacker.new(io)
      begin
        @u.each(&@on_message)
      rescue EOFError
      end
    end
  end
end


end

