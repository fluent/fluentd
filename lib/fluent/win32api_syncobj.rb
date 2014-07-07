
require 'Win32API'
require 'fluent/win32api_constants.rb'

module Fluent
  class Win32Base
    @api_getversionex = nil
    def Win32Base.getversionex
      @api_getversionex = Win32API.new("kernel32","GetVersionExW", %w(p),'i') unless @api_getversionex
      osvi = [276]
      (4*4+2*128).times do
        osvi.push 0
      end
      osvi_pack = osvi.pack("L5S128")
      @api_getversionex.call(osvi_pack)
      return osvi_pack.unpack("L5S128")
    end
  end
  
  class Win32Dll
    @api_getmodulefilename = nil
    def Win32Dll.getmodulefilename
      filename = "\0" * 256
      @api_getmodulefilename = Win32API.new("kernel32", "GetModuleFileName", %w(i p i), 'i') unless @api_getmodulefilename
      @api_getmodulefilename.call(0, filename, 256)
      filename = filename.rstrip
      return filename.gsub(/\\/, '/')
    end
  end
  
  class Win32SyncObj
    def Win32SyncObj.createevent(manual=0, init=0, name=nil)
      w32evt = Win32Event.new
      h = w32evt.open(manual, init, name)
      unless h
        return nil
      end
      return w32evt
    end
  end
  
  class Win32Event
    @event_handle = 0
    @api_createevent = nil
    @api_closehandle = nil
    @api_waitforsingleobject = nil
    @api_setevent = nil
    @api_resetevent = nil
    
    def open(manual, init, name)
      @api_createevent = Win32API.new('kernel32', 'CreateEvent', %w(p i i p), 'i') unless @api_createevent
      @event_handle = @api_createevent.call(nil, manual, init, name)
    end
    
    def close
      @api_closehandle = Win32API.new('kernel32', 'CloseHandle', 'i', 'i') unless @api_closehandle
      @api_closehandle.call(@event_handle)
      @event_handle = 0
    end
    
    def wait(timeoutmilli=0) 
      #timeout cannot be used currently.
      @api_waitforsingleobject = Win32API.new('kernel32.dll', 'WaitForSingleObject', %w(i, i), 'i') unless @api_waitforsingleobject
      result = @api_waitforsingleobject.call(@event_handle, 0)
      while result == 0x102
        result = @api_waitforsingleobject.call(@event_handle, 0)
        sleep(1) 
      end
    end
    
    def signal_on
      @api_setevent = Win32API.new('kernel32', 'SetEvent', 'i', 'i') unless @api_setevent
      return @api_setevent.call(@event_handle) 
    end
    
    def signal_off
      @api_resetevent = Win32API.new('kernel32', 'ResetEvent', 'i', 'i') unless @api_resetevent
      return @api_resetevent.call(@event_handle) 
    end    
  end
end

