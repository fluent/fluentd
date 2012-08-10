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

  class MultiWriter
    def initialize(collector)
      @writers = {}
      @collector = collector
    end

    def append(tag, time, record)
      w = (@writers[tag] ||= @collector.open(tag))
      w.append(tag, time, record)
    end

    def chunk(tag, chunk)
      w = (@writers[tag] ||= @collector.open_stream(tag))
      w.write(tag, record)
    end

    def close
      @writers.values.reverse_each {|w|
        w.close  # TODO log
      }
    end
  end

end
