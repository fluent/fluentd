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

require 'fluent/platform'

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
        f.stat
      ensure
        f&.close
      end
    end

    class WindowsFile
      include File::Constants

      attr_reader :io

      INVALID_HANDLE_VALUE = -1

      def initialize(path, mode_enc='r')
        mode, enc = mode_enc.split(":", 2)
        @io = File.open(path, mode2flags(mode))
        @io.set_encoding(enc) if enc
        @file_handle = Win32API._get_osfhandle(@io.to_i)
        @ino, @dev = file_identity
        @io.instance_variable_set(:@file_index, @ino)
        def @io.ino
          @file_index
        end
      end

      def close
        @io.close
        @file_handle = INVALID_HANDLE_VALUE
      end

      attr_reader :ino

      def stat
        raise Errno::ENOENT if delete_pending
        s = @io.stat
        s.instance_variable_set :@ino, @ino
        s.instance_variable_set :@dev, @dev
        def s.ino; @ino; end
        def s.dev; @dev; end
        s
      end

      private

      # Keep the same file index encoding used by previous versions.
      # Ruby's File.stat uses a different, 128-bit file identifier on Windows.
      def file_identity
        buffer = "\0" * 52
        unless Win32API.GetFileInformationByHandle(@file_handle, buffer)
          return [0, 0]
        end

        # BY_HANDLE_FILE_INFORMATION stores the volume serial number before
        # the file size and link count, and the file index in its last 8 bytes.
        [buffer.unpack("I11Q1")[11], buffer.unpack("I8")[7]]
      end

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
