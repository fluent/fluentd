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

unless Fluent.windows?
  Fluent::FileWrapper = File
else
  require 'fluent/win32api'

  module Fluent
    module FileWrapper
      def self.open(path, mode='r')
        io = WindowsFile.new(path, mode).io
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

    class WindowsFile
      include File::Constants

      attr_reader :io

      INVALID_HANDLE_VALUE = -1

      def initialize(path, mode='r')
        @path = path
        @io = File.open(path, mode2flags(mode))
        @file_handle = Win32API._get_osfhandle(@io.to_i)
        @io.instance_variable_set(:@file_index, self.ino)
        def @io.ino
          @file_index
        end
      end

      def close
        @io.close
        @file_handle = INVALID_HANDLE_VALUE
      end

      # To keep backward compatibility, we continue to use GetFileInformationByHandle()
      # to get file id.
      # Note that Ruby's File.stat uses GetFileInformationByHandleEx() with FileIdInfo
      # and returned value is different with above one, former one is 64 bit while
      # later one is 128bit.
      def ino
        by_handle_file_information = '\0'*(4+8+8+8+4+4+4+4+4+4)   #72bytes

        unless Win32API.GetFileInformationByHandle(@file_handle, by_handle_file_information)
          return 0
        end

        by_handle_file_information.unpack("I11Q1")[11] # fileindex
      end

      def stat
        raise Errno::ENOENT if delete_pending
        s = File.stat(@path)
        s.instance_variable_set :@ino, self.ino
        def s.ino; @ino; end
        s
      end

      private

      def mode2flags(mode)
        # Always inject File::Constants::SHARE_DELETE
        # https://github.com/fluent/fluentd/pull/3585#issuecomment-1101502617
        # To enable SHARE_DELETE, BINARY is also required.
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

        unless Win32API.GetFileInformationByHandleEx(@file_handle, file_standard_info, buf, bufsize)
          return false
        end

        return buf.unpack("QQICC")[3] != 0
      end
    end
  end
end
