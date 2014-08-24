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

module Fluent
  class EventStream
    include Enumerable

    def repeatable?
      false
    end

    def each(&block)
      raise NotImplementedError, "DO NOT USE THIS CLASS directly."
    end

    def to_msgpack_stream
      out = ''
      each {|time,record|
        [time,record].to_msgpack(out)
      }
      out
    end
  end


  class OneEventStream < EventStream
    def initialize(time, record)
      @time = time
      @record = record
    end

    def dup
      OneEventStream.new(@time, @record.dup)
    end

    def repeatable?
      true
    end

    def each(&block)
      block.call(@time, @record)
      nil
    end
  end

  # EventStream from entries: Array of [time, record]
  #
  # Use this class for many events data with a tag
  # and its representation is [ [time, record], [time, record], .. ]
  class ArrayEventStream < EventStream
    def initialize(entries)
      @entries = entries
    end

    def dup
      entries = @entries.map(:dup)
      ArrayEventStream.new(entries)
    end

    def repeatable?
      true
    end

    def empty?
      @entries.empty?
    end

    def each(&block)
      @entries.each(&block)
      nil
    end
  end

  # EventStream from entries: numbers of pairs of time and record.
  #
  # This class can handle many events more efficiently than ArrayEventStream
  # because this class generate less objects than ArrayEventStream.
  #
  # Use this class as below, in loop of data-enumeration:
  #  1. initialize blank stream:
  #     streams[tag] ||= MultiEventStream
  #  2. add events
  #     stream[tag].add(time, record)
  class MultiEventStream < EventStream
    def initialize
      @time_array = []
      @record_array = []
    end

    def dup
      es = MultiEventStream.new
      @time_array.zip(@record_array).each { |time, record|
        es.add(time, record.dup)
      }
      es
    end

    def add(time, record)
      @time_array << time
      @record_array << record
    end

    def repeatable?
      true
    end

    def empty?
      @time_array.empty?
    end

    def each(&block)
      time_array = @time_array
      record_array = @record_array
      for i in 0..time_array.length-1
        block.call(time_array[i], record_array[i])
      end
      nil
    end
  end

  class MessagePackEventStream < EventStream
    # Keep cached_unpacker argument for existence plugins
    def initialize(data, cached_unpacker = nil)
      @data = data
    end

    def repeatable?
      true
    end

    def each(&block)
      # TODO format check
      unpacker = MessagePack::Unpacker.new
      unpacker.feed_each(@data, &block)
      nil
    end

    def to_msgpack_stream
      @data
    end
  end
end

