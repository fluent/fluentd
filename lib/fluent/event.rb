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


class Event
	def initialize(time=0, record={})
		@time = time
		@record = record
	end
	attr_reader :time, :record

	def from_msgpack(msg)
		@time = msg[0].to_i
		@record = msg[1] || {}
		return self
	end

	def to_msgpack(out = '')
		[@time, @record].to_msgpack(out)
	end
end


class EventStream
	def repeatable?
		false
	end

	#def each(&block)
	#end
end


class ArrayEventStream < EventStream
	def initialize(array)
		@array = array
	end

	def repeatable?
		true
	end

	def each(&block)
		@array.each(&block)
	end
end


class RawEventStream < EventStream
	def initialize(data)
		@data = data
	end

	def repeatable?
		true
	end

	def each(&block)
		array = []
		u = MessagePack::Unpacker.new
		u.feed_each(@data) {|obj|
			yield Event.new.from_msgpack(obj)
		}
	end
end


class IoEventStream < EventStream
	def initialize(io, num)
		@io = io
		@num = num
		@u = MessagePack::Unpacker.new(@io)
	end

	def repeatable?
		false
	end

	def each(&block)
		return nil if @num == 0
		@u.each {|obj|
			yield Event.new.from_msgpack(obj)
			break if @array.size >= @num
		}
	end
end


end

