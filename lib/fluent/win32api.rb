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

module Fluent
  module Win32API
    require 'fiddle/import'
    require 'fiddle/types'
    extend Fiddle::Importer

    if RUBY_PLATFORM.split('-')[-1] == "ucrt"
      MSVCRT_DLL = 'ucrtbase.dll'
    else
      MSVCRT_DLL = 'msvcrt.dll'
    end

    dlload MSVCRT_DLL, "kernel32.dll"
    include Fiddle::Win32Types

    extern "intptr_t _get_osfhandle(int)"
    extern "BOOL GetFileInformationByHandle(HANDLE, void *)"
    extern "BOOL GetFileInformationByHandleEx(HANDLE, int, void *, DWORD)"
    extern "BOOL UnlockFileEx(HANDLE, DWORD, DWORD, DWORD, void *)"

    LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
    LOCKFILE_EXCLUSIVE_LOCK = 0x00000002
    ERROR_LOCK_VIOLATION = 33

    # Only for LockFileEx with LOCKFILE_FAIL_IMMEDIATELY, which doesn't block. Keep the GVL during the call.
    # Otherwise Fiddle reads GetLastError after taking the GVL back, and the error code may be lost.
    LOCK_FILE_EX = Fiddle::Function.new(
      handler.sym("LockFileEx"),
      [Fiddle::TYPE_VOIDP, -Fiddle::TYPE_INT32_T, -Fiddle::TYPE_INT32_T, -Fiddle::TYPE_INT32_T, -Fiddle::TYPE_INT32_T, Fiddle::TYPE_VOIDP],
      Fiddle::TYPE_INT,
      need_gvl: true,
    )
    private_constant :LOCK_FILE_EX

    def self.lock_file_ex_nonblocking(handle, flags, reserved, bytes_low, bytes_high, overlapped)
      LOCK_FILE_EX.call(handle, flags | LOCKFILE_FAIL_IMMEDIATELY, reserved, bytes_low, bytes_high, overlapped)
    end
  end if Fluent.windows?
end
