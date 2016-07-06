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

DEBUG_LOG = "C:\\fluentd-service.log"

require 'time'

$logger = ->(message, backtrace=nil){
  File.open(DEBUG_LOG, "a+") do |f|
    f.puts "#{Time.now.iso8601} #{message}"
    backtrace.each{|line| f.puts line } if backtrace
  end
}

begin

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
    $logger.call("service launched, pid:#{@pid}")
    while running?
      $logger.call("service still running")
      sleep 5
    end
    $logger.call("service is now stopping")
    begin
      $logger.call("killing #{@pid} by TERM")
      Process.kill(:TERM, @pid)
    rescue Errno::ECHILD, Errno::ESRCH, Errno::EPERM
      # specified process is already dead
    end
  end

  def service_stop
    begin
      $logger.call("waiting process stopped")
      Porcess.waitpid(@pid)
      $logger.call("child process successfully stopped")
    rescue Errno::ECHILD, Errno::ESRCH, Errno::EPERM
      # specified process is already dead
      $logger.call("child process already dead")
    rescue => e
      $logger.call("unexpected error while waiting child process #{e.class}:#{e.message}")
      raise
    end
    ev = Win32::Event.open(INTEVENTOBJ_NAME)
    ev.set
    ev.close
    $logger.call("service #{INTEVENTOBJ_NAME} successfully stopped")
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

rescue Exception => e
$logger.call("Fluentd service crashed #{e.class}:#{e.message}", e.backtrace)
raise
end
