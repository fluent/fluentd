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


class RoundRobinOutput < MultiOutput
  Plugin.register_output('roundrobin', self)

  def initialize
    @outputs = []
  end

  attr_reader :outputs

  def configure(conf)
    conf.elements.select {|e|
      e.name == 'store'
    }.each {|e|
      type = e['type']
      unless type
        raise ConfigError, "Missing 'type' parameter on <store> directive"
      end
      $log.debug "adding store type=#{type.dump}"

      output = Plugin.new_output(type)
      output.configure(e)
      @outputs << output
    }
    @rr = -1  # starts from @output[0]
  end

  def start
    @outputs.each {|o|
      o.start
    }
  end

  def shutdown
    @outputs.each {|o|
      o.shutdown
    }
  end

  def emit(tag, es, chain)
    next_output.emit(tag, es, chain)
  end

  protected
  def next_output
    @rr = 0 if (@rr += 1) >= @outputs.size
    @outputs[@rr]
  end
end


end

