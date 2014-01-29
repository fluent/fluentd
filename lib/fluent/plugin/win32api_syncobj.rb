
require 'Win32API'
require 'fluent/win32api_constants.rb'

module Fluent
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
      unless @api_createevent
        @api_createevent = Win32API.new('kernel32', 'CreateEvent', %w(p i i p), 'i')
      end
      @event_handle = @api_createevent.call(nil, manual, init, name)
    end
    
    def close
      unless @api_closehandle
        @api_closehandle = Win32API.new('kernel32', 'CloseHandle', 'i', 'i')
      end
      @api_closehandle.call(@event_handle)
      @event_handle = nil
    end
    
    def wait(timeoutmilli)
      unless @api_waitforsingleobject 
        @api_waitforsingleobject = Win32API.new('kernel32.dll', 'WaitForSingleObject', %w(i, i), 'i')
      end
      result = @api_waitforsingleobject.call(@event_handle, 0)
      while result == 0x102
        result = @api_waitforsingleobject.call(@event_handle, 0)
        sleep(1) 
      end
    end
    
    def signal_on
      unless @api_setevent
        @api_setevent = Win32API.new('kernel32', 'SetEvent', 'i', 'i')
      end
      ret = @api_setevent.call(@event_handle) 
      return ret
    end
    
    def signal_off
      unless @api_resetevent
        @api_resetevent = Win32API.new('kernel32', 'ResetEvent', 'i', 'i')
      end
      return @api_resetevent.call(@event_handle) 
    end
    
  end
end

