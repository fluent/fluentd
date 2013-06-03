#
# Fluentd
#
# Copyright (C) 2011-2013 FURUHASHI Sadayuki
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
  module Actors

    class Future
      def initialize
        @set = false
        @result = nil
        @exception = nil
        @mutex = Mutex.new
        @cond = ConditionVariable.new
      end

      def set?
        @set
      end

      def set!(result, exception=nil)
        @mutex.synchronize do
          if @set
            raise "already set"  # TODO error class
          end
          @result = result
          @exception = exception
          @set = true
        end
      end

      def value
        join
        if @exception
          raise @exception
        end
        return @result
      end

      def join
        @mutex.synchronize do
          until @set
            @cond.wait(@mutex)
          end
        end
        self
      end
    end

  end
end
