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


  module Collector
    def open(tag)
    end

    def open_multi
      if block_given?
        w = open_multi
        begin
          yield w
        ensure
          w.close
        end
      else
        return MultiWriter.new(self)
      end
    end

    def append(tag, time, record)
      w = open
      begin
        w.append(tag, time, record)
      ensure
        w.close
      end
    end

    private
    def ensure_close(writer, block)
      begin
        block.yield(writer)
      ensure
        writer.close
      end
    end
  end


end
