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

  require 'msgpack'

  module EventStream
    include Enumerable

    def repeatable?
      destructive_repeatable?
    end

    def destructive_repeatable?
      false
    end

    #def each(&block)
    #end

    def to_msgpack_stream
      out = ''
      each {|time,record|
        [time,record].to_msgpack(out)
      }
      out
    end
  end

  class OneEventStream
    include Enumerable

    def initialize(time, record)
      @time = time
      @record = record
    end

    def destructive_repeatable?
      true
    end

    def each(&block)
      block.call(@time, @record)
    end

    def to_msgpack_stream
      [@time, @record].to_msgpack
    end
  end

  class MultiEventStream
    include EventStream

    def initialize
      @time_array = []
      @record_array = []
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

  class MessagePackEventStream
    include EventStream

    def initialize(data)
      @data = data
    end

    def destructive_repeatable?
      true
    end

    def each(&block)
      MessagePack::Unpacker.new.feed_each(@data, &block)
      nil
    end

    def to_msgpack_stream
      @data
    end

    # for Marshal
    def _dump(level)
      @data
    end

    def self._load(data)
      new(data)
    end
  end

end
