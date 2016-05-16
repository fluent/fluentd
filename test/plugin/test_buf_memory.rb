require_relative '../helper'
require 'fluent/plugin/buf_memory'
require 'fluent/plugin/output'
require 'flexmock/test_unit'

module FluentPluginMemoryBufferTest
  class DummyOutputPlugin < Fluent::Plugin::Output
  end
end

class MemoryBufferTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
    @d = FluentPluginMemoryBufferTest::DummyOutputPlugin.new
    @p = Fluent::Plugin::MemoryBuffer.new
    @p.owner = @d
  end

  test 'this is non persistent plugin' do
    assert !@p.persistent?
  end

  test '#resume always returns empty stage and queue' do
    ary = @p.resume
    assert_equal({}, ary[0])
    assert_equal([], ary[1])
  end

  test '#generate_chunk returns memory chunk instance' do
    m1 = Fluent::Plugin::Buffer::Metadata.new(nil, nil, nil)
    c1 = @p.generate_chunk(m1)
    assert c1.is_a? Fluent::Plugin::Buffer::MemoryChunk
    assert_equal m1, c1.metadata

    require 'time'
    t2 = Time.parse('2016-04-08 19:55:00 +0900').to_i
    m2 = Fluent::Plugin::Buffer::Metadata.new(t2, 'test.tag', {k1: 'v1', k2: 0})
    c2 = @p.generate_chunk(m2)
    assert c2.is_a? Fluent::Plugin::Buffer::MemoryChunk
    assert_equal m2, c2.metadata
  end
end
