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

module Fluent
  module FileWrapper
    include File::Constants
    def self.mode2flags(mode)
      # Always need BINARY to enable SHARE_DELETE
      # https://bugs.ruby-lang.org/issues/11218
      # https://github.com/ruby/ruby/blob/d6684f063bc53e3cab025bd39526eca3b480b5e7/win32/win32.c#L6332-L6345
      flags = BINARY | SHARE_DELETE
      case mode.delete("b")
      when "r"
        flags |= RDONLY
      when "r+"
        flags |= RDWR
      when "w"
        flags |= WRONLY | CREAT | TRUNC
      when "w+"
        flags |= RDWR | CREAT | TRUNC
      when "a"
        flags |= WRONLY | CREAT | APPEND
      when "a+"
        flags |= RDWR | CREAT | APPEND
      else
        raise Errno::EINVAL.new("Unsupported mode by Fluent::FileWrapper: #{mode}")
      end
    end

    def self.open(path, mode='r')
      # inject File::Constants::SHARE_DELETE
      # https://github.com/fluent/fluentd/pull/3585#issuecomment-1101502617
      io = File.open(path, mode2flags(mode))
      if block_given?
        v = yield io
        io.close
        v
      else
        io
      end
    end

    def self.stat(path)
      f = WindowsFile.new(path)
      s = f.stat
      f.close
      s
    end
  end

  class Win32Error < StandardError
    require 'windows/error'
    include Windows::Error

    attr_reader :errcode, :msg

    WSABASEERR = 10000

    def initialize(errcode, msg = nil)
      @errcode = errcode
      @msg = msg
    end

    def format_english_message(errcode)
      buf = 0.chr * 260
      flags = FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_ARGUMENT_ARRAY
      english_lang_id = 1033 # The result of MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US)
      FormatMessageA.call(flags, 0, errcode, english_lang_id, buf, buf.size, 0)
      buf.force_encoding(Encoding.default_external).strip
    end

    def to_s
      msg = super
      msg << ": code: #{@errcode}, #{format_english_message(@errcode)}"
      msg << " - #{@msg}" if @msg
      msg
    end

    def inspect
      "#<#{to_s}>"
    end

    def ==(other)
      return false if other.class != Win32Error
      @errcode == other.errcode && @msg == other.msg
    end

    def wsaerr?
      @errcode >= WSABASEERR
    end
  end

  # To open and get stat with setting FILE_SHARE_DELETE.
  # Although recent Ruby's File.stat uses it, we still need this to keep
  # backward compatibility of ino and delete_pending methods.
  class WindowsFile
    require 'windows/file'
    require 'windows/error'
    require 'windows/handle'
    require 'windows/nio'

    include Windows::Error
    include Windows::File
    include Windows::Handle
    include Windows::NIO

    def initialize(path, mode='r', sharemode=FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE)
      @path = path
      @file_handle = INVALID_HANDLE_VALUE
      @mode = mode


      access, creationdisposition, _seektoend = case mode.delete('b')
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
        win32err = Win32Error.new(Win32::API.last_error, path)
        errno = ServerEngine::RbWinSock.rb_w32_map_errno(win32err.errcode)
        if errno == Errno::EINVAL::Errno || win32err.wsaerr?
          # maybe failed to map
          raise win32err
        else
          raise SystemCallError.new(win32err.message, errno)
        end
      end
    end

    def close
      CloseHandle.call(@file_handle)
      @file_handle = INVALID_HANDLE_VALUE
    end

    def ino
      by_handle_file_information = '\0'*(4+8+8+8+4+4+4+4+4+4)   #72bytes

      unless GetFileInformationByHandle.call(@file_handle, by_handle_file_information)
        return 0
      end

      by_handle_file_information.unpack("I11Q1")[11] # fileindex
    end

    # DeletePending is a Windows-specific file state that roughly means
    # "this file is queued for deletion, so close any open handlers"
    #
    # This flag can be retrieved via GetFileInformationByHandleEx().
    #
    # https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-getfileinformationbyhandleex
    #
    def delete_pending
      file_standard_info = 0x01
      bufsize = 1024
      buf = '\0' * bufsize

      unless GetFileInformationByHandleEx.call(@file_handle, file_standard_info, buf, bufsize)
        return false
      end

      return buf.unpack("QQICC")[3] != 0
    end

    private :delete_pending

    def stat
      raise Errno::ENOENT if delete_pending
      s = File.stat(@path)
      s.instance_variable_set :@ino, self.ino
      def s.ino; @ino; end
      s
    end
  end
end if Fluent.windows?
