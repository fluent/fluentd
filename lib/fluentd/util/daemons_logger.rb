#
# Fluentd
#
# Copyright (C) 2012 FURUHASHI Sadayuki
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

module Fluentd

  require 'logger'

  class DaemonsLogger < Logger
    def initialize(dev, shift_age=0, shift_size=1048576)
      @stdout_hook = false
      @stderr_hook = false
      if dev.is_a?(String)
        @path = dev
        @io = File.open(@path, File::WRONLY|File::APPEND|File::CREAT)
      else
        @io = dev
      end
      super(@io, shift_size, shift_size)
    end

    def hook_stdout!
      return nil if @io == STDOUT
      STDOUT.reopen(@io)
      @stdout_hook = true
      self
    end

    def hook_stderr!
      STDERR.reopen(@io)
      @stderr_hook = true
      self
    end

    def reopen!
      if @path
        @io.reopen(@path)
        if @stdout_hook
          STDOUT.reopen(@io)
        end
        if @stderr_hook
          STDERR.reopen(@io)
        end
      end
      nil
    end

    def reopen
      begin
        reopen!
        return true
      rescue
        # TODO log?
        return false
      end
    end

    def close
      if @path
        @io.close unless @io.closed?
      end
      nil
    end
  end

end
