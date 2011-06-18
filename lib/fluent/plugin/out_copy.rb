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


class CopyOutput < Output
  Plugin.register_output('copy', self)

  def initialize
    @stores = []
  end

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

      @stores << output
    }
  end

  def start
    @stores.each {|s|
      s.start
    }
  end

  def shutdown
    @stores.each {|s|
      s.shutdown
    }
  end

  def emit(tag, es, chain)
    unless es.repeatable?
      array = []
      es.each {|e|
        e << array
      }
      es = ArrayEventStream.new(array)
    end
    chain = OutputChain.new(@stores, tag, es, chain)
    chain.next
  end
end


end

