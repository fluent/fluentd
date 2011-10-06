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
module Test


class OutputTestDriver < TestDriver
  # TODO
end


class BufferedOutputTestDriver < TestDriver
  def initialize(klass, tag='test', &block)
    super(klass, &block)
    @emits = []
    @expected_buffer = nil
    @tag = tag
  end

  attr_accessor :tag

  def emit(record, time=Time.now)
    @emits << Event.new(time.to_i, record)
    self
  end

  def expect_format(str)
    (@expected_buffer ||= '') << str
  end

  def run
    super {
      es = ArrayEventStream.new(@emits)
      buffer = @instance.format_stream(@tag, es)

      if @expected_buffer
        assert_equal(@expected_buffer, buffer)
      end

      chunk = MemoryBufferChunk.new('', buffer)
      @instance.write(chunk)
    }
  end
end


end
end

