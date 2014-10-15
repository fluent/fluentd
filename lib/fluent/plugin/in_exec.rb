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

module Fluent
  class ExecInput < Input
    Plugin.register_input('exec', self)

    def initialize
      super
      require 'fluent/plugin/exec_util'
      require 'fluent/timezone'
    end

    SUPPORTED_FORMAT = {
      'tsv' => :tsv,
      'json' => :json,
      'msgpack' => :msgpack,
    }

    config_param :command, :string
    config_param :format, :default => :tsv do |val|
      f = SUPPORTED_FORMAT[val]
      raise ConfigError, "Unsupported format '#{val}'" unless f
      f
    end
    config_param :keys, :default => [] do |val|
      val.split(',')
    end
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

      if conf['timezone']
        @timezone = conf['timezone']
        Fluent::Timezone.validate!(@timezone)
      end

      if !@tag && !@tag_key
        raise ConfigError, "'tag' or 'tag_key' option is required on exec input"
      end

      if @time_key
        if @time_format
          f = @time_format
          @time_parse_proc = Proc.new {|str| Time.strptime(str, f).to_i }
        else
          @time_parse_proc = Proc.new {|str| str.to_i }
        end
      end

      case @format
      when :tsv
        if @keys.empty?
          raise ConfigError, "keys option is required on exec input for tsv format"
        end
        @parser = ExecUtil::TSVParser.new(@keys, method(:on_message))
      when :json
        @parser = ExecUtil::JSONParser.new(method(:on_message))
      when :msgpack
        @parser = ExecUtil::MessagePackParser.new(method(:on_message))
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
        begin
          Process.kill(:TERM, @pid)
        rescue #Errno::ECHILD, Errno::ESRCH, Errno::EPERM
        end
        if @thread.join(60)  # TODO wait time
          return
        end

        begin
          Process.kill(:KILL, @pid)
        rescue #Errno::ECHILD, Errno::ESRCH, Errno::EPERM
        end
        @thread.join
      end
    end

    def run
      @parser.call(@io)
    end

    def run_periodic
      until @finished
        begin
          sleep @run_interval
          io = IO.popen(@command, "r")
          @parser.call(io)
          Process.waitpid(io.pid)
        rescue
          log.error "exec failed to run or shutdown child process", :error => $!.to_s, :error_class => $!.class.to_s
          log.warn_backtrace $!.backtrace
        end
      end
    end

    private

    def on_message(record)
      if val = record.delete(@tag_key)
        tag = val
      else
        tag = @tag
      end

      if val = record.delete(@time_key)
        time = @time_parse_proc.call(val)
      else
        time = Engine.now
      end

      router.emit(tag, time, record)
    rescue => e
      log.error "exec failed to emit", :error => e.to_s, :error_class => e.class.to_s, :tag => tag, :record => Yajl.dump(record)
    end
  end
end
