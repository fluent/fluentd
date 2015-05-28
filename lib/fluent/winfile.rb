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

# https://github.com/djberg96/win32-api
require 'win32/api'
module Win32
  class API
    DLL = {}
    TYPEMAP = {
      "0" => Fiddle::TYPE_VOID,
      "V" => Fiddle::TYPE_VOID,
      "S" => Fiddle::TYPE_VOIDP,
      "I" => Fiddle::TYPE_LONG,
      "L" => Fiddle::TYPE_LONG_LONG,
      "P" => Fiddle::TYPE_VOIDP,
      "B" => Fiddle::TYPE_CHAR,
      "K" => Fiddle::TYPE_VOIDP, # callback
    }
    POINTER_TYPE = Fiddle::SIZEOF_VOIDP == 8 ? 'q*' : 'l!*'

    def initialize(func, import, export = "0", dllname)
      calltype = :stdcall
      @proto = [import].join.tr("VPpNnLlIi", "0SSI").sub(/^(.)0*$/, '\1')
      handle = DLL[dllname] ||= Fiddle.dlopen(dllname)
      @func = Fiddle::Function.new(
                handle[func],
                @proto.chars.map { |win_type| TYPEMAP[win_type] },
                TYPEMAP.fetch(export)
                )
    rescue Fiddle::DLError => e
      raise RuntimeError, e.message, e.backtrace
    end

    def call(*args)
      import = @proto.split("")
      args.each_with_index do |x, i|
        if TYPEMAP[import[i]] == Fiddle::TYPE_VOIDP
          args[i], = [x == 0 ? nil : x].pack("p").unpack(POINTER_TYPE)
        end
        args[i], = [x].pack("I").unpack("i") if import[i] == "I"
      end
      args = [nil] if args.empty?
      ret, = @func.call(*args)
      ret || 0
    end
  end
end

module Fluent
  require 'windows/file'
  require 'windows/error'
  require 'windows/handle'
  require 'windows/nio'

  class Win32File
    include Windows::Error
    include Windows::File
    include Windows::Handle
    include Windows::NIO

    def self.open(*args)
      f = self.new(*args)
      io = f.io
      if block_given?
        v = yield io
        io.close
        v
      else
        io
      end
    end

    def initialize(path, mode='r', sharemode=FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE)
      @path = path
      @file_handle = INVALID_HANDLE_VALUE
      @mode = mode


      access, creationdisposition, seektoend = case mode.delete('b')
      when "r" ; [FILE_GENERIC_READ                     , OPEN_EXISTING, false]
      when "r+"; [FILE_GENERIC_READ | FILE_GENERIC_WRITE, OPEN_ALWAYS  , false]
      when "w" ; [FILE_GENERIC_WRITE                    , CREATE_ALWAYS, false]
      when "w+"; [FILE_GENERIC_READ | FILE_GENERIC_WRITE, CREATE_ALWAYS, false]
      when "a" ; [FILE_GENERIC_WRITE                    , OPEN_ALWAYS  , true]
      when "a+"; [FILE_GENERIC_READ | FILE_GENERIC_WRITE, OPEN_ALWAYS  , true]
      else raise "unknown mode '#{mode}'"
      end

      @file_handle = CreateFile.call(@path, access, sharemode,
                     0, creationdisposition, FILE_ATTRIBUTE_NORMAL, 0)
      if @file_handle == INVALID_HANDLE_VALUE
        err = GetLastError.call
	if err == ERROR_FILE_NOT_FOUND || err == ERROR_ACCESS_DENIED
          raise SystemCallError.new(2)
        end
        raise SystemCallError.new(err)
      end
    end

    def close
      CloseHandle.call(@file_handle)
      @file_handle = INVALID_HANDLE_VALUE
    end

    def io
      fd = _open_osfhandle(@file_handle, 0)
      raise Errno::ENOENT if fd == -1
      io = File.for_fd(fd, @mode)

      io.instance_variable_set :@ino, self.ino
      io.instance_variable_set :@path, @path
      class << io
        alias orig_stat stat
	attr_reader :path
        def stat
	  s = orig_stat
	  s.instance_variable_set :@ino, @ino
	  def s.ino; @ino; end
	  s
	end
      end
      io
    end

    def ino
      by_handle_file_information = '\0'*(4+8+8+8+4+4+4+4+4+4)   #72bytes

      unless GetFileInformationByHandle.call(@file_handle, by_handle_file_information)
        return 0
      end

      fileindex = by_handle_file_information.unpack("I11Q1")[11]
      return fileindex
    end

    def self.stat(path)
      f = self.new(path)
      s = f.stat
      f.close
      s
    end

    def stat
      s = File.stat(@path)
      s.instance_variable_set :@ino, self.ino
      def s.ino; @ino; end
      s
    end
  end
end
