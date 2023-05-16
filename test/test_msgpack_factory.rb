require_relative 'helper'
require 'fluent/msgpack_factory'

class MessagePackFactoryTest < Test::Unit::TestCase
  test 'call log.warn only once' do
    klass = Class.new do
      include Fluent::MessagePackFactory::Mixin
    end

    mp = klass.new

    mock.proxy($log).warn(anything).once

    assert mp.msgpack_factory
    assert mp.msgpack_factory
    assert mp.msgpack_factory
  end

  sub_test_case 'thread_local_msgpack_packer' do
    test 'packer is cached' do
      packer1 = Fluent::MessagePackFactory.thread_local_msgpack_packer
      packer2 = Fluent::MessagePackFactory.thread_local_msgpack_packer
      assert_equal packer1, packer2
    end
  end

  sub_test_case 'thread_local_msgpack_unpacker' do
    test 'unpacker is cached' do
      unpacker1 = Fluent::MessagePackFactory.thread_local_msgpack_unpacker
      unpacker2 = Fluent::MessagePackFactory.thread_local_msgpack_unpacker
      assert_equal unpacker1, unpacker2
    end

    # We need to reset the buffer every time so that received incomplete data
    # must not affect data from other senders.
    test 'reset the internal buffer of unpacker every time' do
      unpacker1 = Fluent::MessagePackFactory.thread_local_msgpack_unpacker
      unpacker1.feed_each("\xA6foo") do |result|
        flunk("This callback must not be called since the data is uncomplete.")
      end

      records = []
      unpacker2 = Fluent::MessagePackFactory.thread_local_msgpack_unpacker
      unpacker2.feed_each("\xA3foo") do |result|
        records.append(result)
      end
      assert_equal ["foo"], records
    end
  end
end
