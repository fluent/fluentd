#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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
module Fluent


class BucketOutput < Output
  # TODO
  #Plugin.register_output('bucket', self)

  def initialize
    @buckets = []
  end

  def configure(conf)
    conf.elements.select {|e|
      e.name == 'bucket'
    }.each {|e|
        output = Plugin.new_output(e.arg)
        output.configure(conf+e)
        @buckets << output
      }
      @rr = 0  # TODO BucketOutput algorithm
  end

  def start
    @buckets.each {|b|
      b.start
    }
  end

  def shutdown
    @buckets.each {|b|
      b.shutdown
    }
  end

  def emit(tag, es, chain)
    b = next_bucket
    @buckets[b].emit(tag, es, chain)
  end

  protected
  def next_bucket
    @rr = 0 if (@rr += 1) >= @buckets.size
  end
end


end

