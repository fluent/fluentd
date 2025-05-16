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

require 'fluent/env'

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
    extern 'int _setmaxstdio(int)'
    extern 'int _getmaxstdio(void)'
  end if Fluent.windows?
end
