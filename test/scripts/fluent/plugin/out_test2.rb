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

module Fluent::Plugin
  class Test2Output < Output
    Fluent::Plugin.register_output('test2', self)

    helpers :event_emitter

    config_param :name, :string

    config_section :buffer do
      config_set_default :chunk_keys, ['tag']
    end

    def initialize
      super
      @emit_streams = []
    end

    attr_reader :emit_streams

    def emits
      all = []
      @emit_streams.each {|tag,events|
        events.each {|time,record|
          all << [tag, time, record]
        }
      }
      all
    end

    def events
      all = []
      @emit_streams.each {|tag,events|
        all.concat events
      }
      all
    end

    def records
      all = []
      @emit_streams.each {|tag,events|
        events.each {|time,record|
          all << record
        }
      }
      all
    end

    def prefer_buffered_processing
      false
    end

    def process(tag, es)
      @emit_streams << [tag, es.to_a]
    end

    def write(chunk)
      es = Fluent::ArrayEventStream.new
      chunk.each do |time, record|
        es.add(time, record)
      end
      @emit_streams << [tag, es]
    end
  end
end
