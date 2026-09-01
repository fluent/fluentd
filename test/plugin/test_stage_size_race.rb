require_relative '../helper'
require 'fluent/plugin/buffer'
require 'fluent/plugin/buffer/memory_chunk'
require 'fluent/plugin_id'
require 'fluent/log'

module StageSizeRace
  class Owner < Fluent::Plugin::Base
    include Fluent::PluginId
    include Fluent::PluginLoggerMixin
  end

  class Buf < Fluent::Plugin::Buffer
    def create_metadata(timekey = nil, tag = nil, variables = nil)
      Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
    end

    def resume
      return {}, []
    end

    def generate_chunk(metadata)
      Fluent::Plugin::Buffer::MemoryChunk.new(metadata)
    end
  end
end

class StageSizeRaceTest < ::Test::Unit::TestCase
  test 'stage_size does not go negative while a write is pending' do
    b = StageSizeRace::Buf.new
    b.owner = StageSizeRace::Owner.new
    b.configure(config_element('buffer', '', { 'total_limit_size' => 1024, 'chunk_limit_size' => 4096 }))
    b.start

    m = b.create_metadata
    b.write({ m => ['a' * 400] })
    chunk = b.stage[m]
    assert_equal 400, b.stage_size

    reached = Queue.new
    resume = Queue.new
    armed = false

    chunk.define_singleton_method(:mon_exit) do
      result = super()
      if armed
        armed = false
        reached << true
        resume.pop
      end
      result
    end

    armed = true
    writer = Thread.new { b.write({ m => ['b' * 400] }) }
    reached.pop

    b.enqueue_chunk(m)
    assert_equal 0, b.stage_size

    resume << true
    writer.join

    assert_equal 0, b.stage.size
    assert_equal 0, b.stage_size
    assert_equal 800, b.queue_size
  ensure
    if writer&.alive?
      resume << true
      writer.join
    end
    b&.stop
  end
end
