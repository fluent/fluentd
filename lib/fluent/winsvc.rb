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
  require 'windows/debug'
  require 'Windows/Library'
  require 'win32/daemon'
  require 'win32/event'

  include Win32
  include Windows::Library
  include Windows::Debug

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
    require 'win32/Registry'
    Win32::Registry::HKEY_LOCAL_MACHINE.open("SYSTEM\\CurrentControlSet\\Services\\#{service_name}") do |reg|
      reg.read("fluentdopt")[1] rescue ""
    end
  end

  def service_main_start(service_name)
    ruby_path = 0.chr * 260
    GetModuleFileName.call(0, ruby_path,260)
    ruby_path = ruby_path.rstrip.gsub(/\\/, '/')
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
      ev = Win32::Event.open(@service_name)
      ev.set
      ev.close
      if @pid > 0
        Process.waitpid(@pid)
      end
    end
  end

  FluentdService.new(opts[:service_name]).mainloop

rescue Exception => err
  raise
end
