#
# BlockingFlag
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

  class BlockingFlag
    def initialize
      require 'thread'
      @set = false
      @mutex = Mutex.new
      @cond = ConditionVariable.new
    end

    def set!
      toggled = false
      @mutex.synchronize do
        unless @set
          @set = true
          toggled = true
        end
        @cond.broadcast
      end
      return toggled
    end

    def reset!
      toggled = false
      @mutex.synchronize do
        if @set
          @set = false
          toggled = true
        end
        @cond.broadcast
      end
      return toggled
    end

    def set?
      @set
    end

    def set_region(&block)
      set!
      begin
        block.call
      ensure
        reset!
      end
    end

    def reset_region(&block)
      reset!
      begin
        block.call
      ensure
        set!
      end
    end

    def wait(timeout=nil)
      @mutex.synchronize do
        @cond.wait(@mutex, timeout)
      end
      self
    end
  end

end

