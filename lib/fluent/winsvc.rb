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

require 'win32/registry'

# require 'windows/debug' # windows-pr
# require 'windows/library' # windows-pr
require 'win32/api'
require 'win32/daemon' # win32-service
require 'win32/event'

# include Win32
# include Windows::Library
# include Windows::Debug

class FluentdService < Win32::Daemon
  INTEVENTOBJ_NAME = "fluentdwinsvc"

  def initialize
    super
    @pid = 0
  end

  def service_main
    @pid = fluentd_process_launch
    while running?
      sleep 5
    end
    begin
      Process.kill(:TERM, @pid)
    rescue Errno::ECHILD, Errno::ESRCH, Errno::EPERM
      # specified process is already dead
    end
  end

  def service_stop
    begin
      Porcess.waitpid(@pid)
    rescue Errno::ECHILD, Errno::ESRCH, Errno::EPERM
      # specified process is already dead
    end
    ev = Win32::Event.open(INTEVENTOBJ_NAME)
    ev.set
    ev.close
  end

  def fluentd_cmdline_option
    Win32::Registry::HKEY_LOCAL_MACHINE.open("SYSTEM\\CurrentControlSet\\Services\\fluentdwinsvc") do |reg|
      reg.read("fluentdopt")[1]
    end
  end

  def fluentd_process_launch
    ruby_path = 0.chr * 260
    # win32/api
    Win32::API.new('GetModuleFileName', 'LPL', 'L').call(0, ruby_path, 260)
    ruby_path = ruby_path.rstrip.gsub(/\\/, '/')
    rubybin_dir = ruby_path[0, ruby_path.rindex("/")]
    opt = fluentd_cmdline_option
    Process.spawn(rubybin_dir + "/ruby.exe " + rubybin_dir + "/fluentd " + opt + " -x " + INTEVENTOBJ_NAME)
  end
end

FluentdService.mainloop
