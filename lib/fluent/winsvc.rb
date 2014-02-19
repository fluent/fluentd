
INTEVENTOBJ_NAME="fluentdwinsvc"

module Fluent

begin
require "win32/daemon"
require "fluent/env"
require "fluent/win32api_syncobj"

include Win32
 
class FluentdService < Daemon
  def service_main
    Process.spawn (Fluent::RUBY_INSTALL_DIR+"/bin/ruby.exe '"+Fluent::RUBY_INSTALL_DIR+"/bin/fluentd' "+Fluent::FLUENTD_OPTION_FOR_WINSVC+" -x "+INTEVENTOBJ_NAME)
    while running?
      sleep 10
    end
  end
 
  def service_stop
    sigint = Win32SyncObj.createevent(1,0,INTEVENTOBJ_NAME)
    sigint.signal_on
    sigint.close
    exit!
  end
end

FluentdService.mainloop

rescue Execption => err
  raise
end

end
