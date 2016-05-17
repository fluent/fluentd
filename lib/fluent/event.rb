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

require 'fluent/msgpack_factory'

module Fluent
  class EventStream
    include Enumerable
    include MessagePackFactory::Mixin

    def size
      raise NotImplementedError, "DO NOT USE THIS CLASS directly."
    end
    alias :length :size

    def repeatable?
      false
    end

    def each(&block)
      raise NotImplementedError, "DO NOT USE THIS CLASS directly."
    end

    def to_msgpack_stream(time_int: false)
      return to_msgpack_stream_forced_integer if time_int
      out = msgpack_packer
      each {|time,record|
        out.write([time,record])
      }
      out.to_s
    end

    def to_msgpack_stream_forced_integer
      out = msgpack_packer
      each {|time,record|
        out.write([time.to_i,record])
      }
      out.to_s
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

    def size
      1
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
      entries = @entries.map { |entry| entry.dup } # @entries.map(:dup) doesn't work by ArgumentError
      ArrayEventStream.new(entries)
    end

    def size
      @entries.size
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
  #     streams[tag] ||= MultiEventStream.new
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

    def size
      @time_array.size
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
    def initialize(data, cached_unpacker = nil, size = 0)
      @data = data
      @size = size
    end

    def size
      @size
    end

    def repeatable?
      true
    end

    def each(&block)
      msgpack_unpacker.feed_each(@data, &block)
      nil
    end

    def to_msgpack_stream(time_int: false)
      # time_int is always ignored because @data is always packed binary in this class
      @data
    end
  end

  module ChunkMessagePackEventStreamer
    include MessagePackFactory::Mixin
    # chunk.extend(ChunkEventStreamer)
    #  => chunk.each{|time, record| ... }
    def each(&block)
      open do |io|
        msgpack_unpacker(io).each(&block)
      end
      nil
    end
    alias :msgpack_each :each

    def to_msgpack_stream(time_int: false)
      # time_int is always ignored because data is already packed and written in chunk
      read
    end
  end
end
