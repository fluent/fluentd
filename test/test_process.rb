require_relative 'helper'
require 'fluent/test'
require 'fluent/event'
require 'fluent/process'
require 'stringio'
require 'msgpack'

module FluentProcessTest
  class DelayedForwarderTest < Test::Unit::TestCase
    include Fluent

    test 'run and emit' do
      io = StringIO.new
      fwd = Fluent::DetachProcessManager::DelayedForwarder.new(io, 0.001)

      num_events_per_tag = 5000
      num_tags = 20

      now = Time.now.to_i
      record = {'key' => 'value'}
      (num_tags * num_events_per_tag).times do |i|
        tag = "foo.bar#{i % num_tags}"
        fwd.emit(tag, OneEventStream.new(now, record))
      end
      sleep 0.5

      io.pos = 0

      tags = {}
      MessagePack::Unpacker.new(io).each do |tag_and_msgpacks|
        tag, ms = *tag_and_msgpacks
        tags[tag] ||= ''
        tags[tag] << ms
      end

      assert_equal(num_tags, tags.size)
      num_tags.times do |i|
        tag = "foo.bar#{i % num_tags}"
        ms = tags[tag]
        count = 0
        MessagePack::Unpacker.new(StringIO.new(ms)).each do |x|
          count += 1
        end
        assert_equal(num_events_per_tag, count)
      end
    end
  end
end
