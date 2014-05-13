
INTEVENTOBJ_NAME="fluentdwinsvc"

module Fluent

begin
require "win32/daemon"
require "fluent/env"
require "fluent/win32api_syncobj"

include Win32
 
class FluentdService < Daemon
  @pid=0
  def service_main
    ruby_path = Fluent::Win32Dll.getmodulefilename
    rubybin_dir = ruby_path[0, ruby_path.rindex("/")]
    @pid=Process.spawn (rubybin_dir+"/ruby.exe '"+rubybin_dir+"/fluentd' "+Fluent::FLUENTD_OPTION_FOR_WINSVC+" -x "+INTEVENTOBJ_NAME)
    while running?
      sleep 10
    end
  end
 
  def service_stop
    sigint = Win32SyncObj.createevent(1,0,INTEVENTOBJ_NAME)
    sigint.signal_on
    sigint.close
    if @pid > 0 
      Process.waitpid(@pid)
    end
  end
end

FluentdService.mainloop

rescue Execption => err
  raise
end

end
