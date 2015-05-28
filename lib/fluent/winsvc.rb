
INTEVENTOBJ_NAME="fluentdwinsvc"

begin

require 'windows/debug'
require 'Windows/Library'
require 'win32/daemon'
require 'win32/event'

include Win32
include Windows::Library
include Windows::Debug

def read_fluentdopt
  require 'win32/Registry'
  Win32::Registry::HKEY_LOCAL_MACHINE.open("SYSTEM\\CurrentControlSet\\Services\\fluentdwinsvc") do |reg|
    reg.read("fluentdopt")[1]
  end
end

def service_main_start
  ruby_path = 0.chr * 260
  GetModuleFileName.call(0, ruby_path,260)
  ruby_path = ruby_path.rstrip.gsub(/\\/, '/')
  rubybin_dir = ruby_path[0, ruby_path.rindex("/")]
  opt = read_fluentdopt
  Process.spawn (rubybin_dir+"/ruby.exe "+rubybin_dir+"/fluentd "+opt+" -x "+INTEVENTOBJ_NAME)
end

class FluentdService < Daemon
  @pid=0
  def service_main
    opt = read_fluentdopt
    @pid=service_main_start
    while running?
      sleep 10
    end
  end

  def service_stop
    ev = Win32::Event.open(INTEVENTOBJ_NAME)
    ev.set
    ev.close
    if @pid > 0
      Porcess.waitpid(@pid)
    end
  end
end

FluentdService.mainloop

rescue Execption => err
  raise
end

