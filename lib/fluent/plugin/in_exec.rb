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


class ExecInput < Input
  Plugin.register_input('exec', self)

  def initialize
    super
  end

  config_param :command, :string
  config_param :keys, :string
  config_param :tag, :string, :default => nil
  config_param :tag_key, :string, :default => nil
  config_param :time_key, :string, :default => nil
  config_param :time_format, :string, :default => nil
  config_param :run_interval, :time, :default => nil

  def configure(conf)
    super

    if localtime = conf['localtime']
      @localtime = true
    elsif utc = conf['utc']
      @localtime = false
    end

    if !@tag && !@tag_key
      raise ConfigError, "'tag' or 'tag_key' option is required on exec input"
    end

    @keys = @keys.split(',')

    if @time_key
      if @time_format
        f = @time_format
        @time_parse_proc = Proc.new {|str| Time.strptime(str, f).to_i }
      else
        @time_parse_proc = Proc.new {|str| str.to_i }
      end
    end
  end

  def start
    if @run_interval
      @finished = false
      @thread = Thread.new(&method(:run_periodic))
    else
      @io = IO.popen(@command, "r")
      @pid = @io.pid
      @thread = Thread.new(&method(:run))
    end
  end

  def shutdown
    if @run_interval
      @finished = true
      @thread.join
    else
      Process.kill(:TERM, @pid)
      if @thread.join(60)  # TODO wait time
        return
      end
      Process.kill(:KILL, @pid)
      @thread.join
    end
  end

  def run
    @io.each_line(&method(:each_line))
  end

  def run_periodic
    until @finished
      sleep @run_interval
      io = IO.popen(@command, "r")
      io.each_line(&method(:each_line))
      Process.waitpid(io.pid)
    end
  end

  private
  def each_line(line)
    begin
      line.chomp!
      vals = line.split("\t")

      tag = @tag
      time = nil
      record = {}
      for i in 0..@keys.length-1
        key = @keys[i]
        val = vals[i]
        if key == @time_key
          time = @time_parse_proc.call(val)
        elsif key == @tag_key
          tag = val
        else
          record[key] = val
        end
      end

      if tag
        time ||= Engine.now
        Engine.emit(tag, time, record)
      end
    rescue
      $log.error "exec failed to emit", :error=>$!.to_s, :line=>line
      $log.warn_backtrace $!.backtrace
    end
  end
end


end
