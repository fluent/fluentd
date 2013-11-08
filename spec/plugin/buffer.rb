require 'spec_helper'
require 'fluentd/engine'
require 'fluentd/plugin/buffer'

describe Fluentd::Plugin::Buffer do
  before :all do
    Fluentd::Engine.load_plugin_api!
  end

  let :buffer do
    Fluentd::Engine.plugins.new_buffer(:memory)
  end

  it 'buffers data for each key' do
    buffer.open do |a|
      a.append('k1', 'v1')
      a.append('k1', 'v2')
      a.append('k2', 'v3')
    end

    chunks = []
    buffer.acquire {|chunk| chunks << chunk }
    buffer.acquire {|chunk| chunks << chunk }

    chunks[0].read.should == "v1v2"
    chunks[1].read.should == "v3"
  end

  it 'flushes buffer for each buffer_chunk_limit' do
    buffer.buffer_chunk_limit = 2

    buffer.open do |a|
      a.append('k1', 'v1')
      a.append('k1', 'v2')
      a.append('k1', 'v3')
    end

    chunks = []
    buffer.acquire {|chunk| chunks << chunk }
    buffer.acquire {|chunk| chunks << chunk }
    buffer.acquire {|chunk| chunks << chunk }

    # total size of a buffer doesn't exceed buffer_chunk_limit
    chunks[0].read.should == "v1"
    chunks[1].read.should == "v2"
    chunks[2].read.should == "v3"
  end

  it 'is thread-safe' do
    buffer.buffer_chunk_limit = 100 * 20 * 2

    (1..100).map do
      Thread.new {
        buffer.open do |a|
          20.times {|i| a.append("k", "#{i}") }
        end
      }
    end.each do |t|
      t.join
    end

    chunks = []
    buffer.acquire {|chunk| chunks << chunk }

    chunks[0].read.should == (0..19).to_a.join * 100
  end
end

