#
# Fluentd
#
# Copyright (C) 2011-2012 FURUHASHI Sadayuki
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
  module Writers

    class SimpleWriter
      include Writer

      def initialize(target, tag, &close_block)
        @target = target
        @tag = tag
        @close_block = close_block
      end

      def append(time, record)
        @target.append(@tag, time, record)
        nil
      end

      def write(chunk)
        @target.write(@tag, chunk)
        nil
      end

      def close
        @close_block.call if @close_block
        nil
      end
    end

  end
end
