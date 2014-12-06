
INTEVENTOBJ_NAME="fluentdwinsvc"

module Fluent

begin
require "win32/daemon"
require 'windows/library'
require 'win32/ipc'
require 'win32/event'

include Windows::Library

include Win32
 
class FluentdService < Daemon
  @pid=0
  def service_main
    ruby_path = "\0" * 256
    GetModuleFileName.call(0,ruby_path,256)
    ruby_path = ruby_path.rstrip.gsub(/\\/, '/')
    rubybin_dir = ruby_path[0, ruby_path.rindex("/")]
    @pid=Process.spawn (rubybin_dir+"/ruby.exe '"+rubybin_dir+"/fluentd' "+Fluent::FLUENTD_OPTION_FOR_WINSVC+" -x "+INTEVENTOBJ_NAME)
    while running?
      sleep 10
    end
  end
 
  def service_stop
    sigint = Win32::Event.open(INTEVENTOBJ_NAME)
    sigint.set
    sigint.close
    if @pid > 0
      Porcess.waitpid(@pid)
    end
  end
end

FluentdService.mainloop

rescue Execption => err
  raise
end

end
