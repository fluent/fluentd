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
end
