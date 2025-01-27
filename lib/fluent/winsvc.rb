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

begin

  require 'optparse'
  require 'win32/daemon'
  require 'win32/event'
  require 'win32/registry'
  require 'serverengine'

  include Win32

  op = OptionParser.new
  opts = {service_name: nil}
  op.on('--service-name NAME', "The name of the Windows Service") {|name|
    opts[:service_name] = name
  }
  op.parse(ARGV)
  if opts[:service_name] == nil
    raise "Error: No Windows Service name set. Use '--service-name'"
  end 

  def read_fluentdopt(service_name)
    Win32::Registry::HKEY_LOCAL_MACHINE.open("SYSTEM\\CurrentControlSet\\Services\\#{service_name}") do |reg|
      reg.read("fluentdopt")[1] rescue ""
    end
  end

  def service_main_start(service_name)
    ruby_path = ServerEngine.ruby_bin_path
    rubybin_dir = ruby_path[0, ruby_path.rindex("/")]
    opt = read_fluentdopt(service_name)
    Process.spawn("\"#{rubybin_dir}/ruby.exe\" \"#{rubybin_dir}/fluentd\" #{opt} -x #{service_name}")
  end

  class FluentdService < Daemon
    @pid = 0
    @service_name = ''

    def initialize(service_name)
      @service_name = service_name
    end
    
    def service_main
      @pid = service_main_start(@service_name)
      while running?
        sleep 10
      end
    end

    def service_stop
      if @pid <= 0
        set_event(@service_name)
        return
      end

      repeat_set_event_several_times_until_success(@service_name)
      Process.waitpid(@pid)
    end

    def service_paramchange
      set_event("#{@service_name}_USR2")
    end

    def service_user_defined_control(code)
      case code
      when 128
        set_event("#{@service_name}_HUP")
      when 129
        set_event("#{@service_name}_USR1")
      when 130
        set_event("#{@service_name}_CONT")
      end
    end

    private

    def set_event(event_name)
      ev = Win32::Event.open(event_name)
      ev.set
      ev.close
    end

    def repeat_set_event_several_times_until_success(event_name)
      retries = 0
      max_retries = 10
      delay_sec = 3

      begin
        set_event(event_name)
      rescue Errno::ENOENT
        # This error occurs when the supervisor process has not yet created the event.
        # If STOP is immediately executed, this state will occur.
        # Retry `set_event' to wait for the initilization of the supervisor.
        retries += 1
        raise if max_retries < retries
        sleep(delay_sec)
        retry
      end
    end
  end

  FluentdService.new(opts[:service_name]).mainloop

rescue Exception => err
  raise
end
