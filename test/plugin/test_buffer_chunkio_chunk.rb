require_relative '../helper'
require 'fluent/plugin/buffer/chunkio_chunk'

class BufferChunkioChunkTest < Test::Unit::TestCase
  include Fluent::Plugin::Compressable

  Metadata = Struct.new(:timekey, :tag, :variables)
  def gen_metadata(timekey: nil, tag: nil, variables: nil)
    Metadata.new(timekey, tag, variables)
  end

  setup do
    Fluent::Test.setup

    @chunkdir = File.expand_path('../../tmp/buffer_chunkio_chunk', __dir__)
    @stream_name = 'buffer'
    @chunk_path = File.join(@chunkdir, @stream_name, 'cio.*.buf')

    @chunkio = ChunkIO.new(context_path: @chunkdir, stream_name: @stream_name)

    FileUtils.mkdir_p(@chunkdir)
  end

  teardown do
    FileUtils.rm_r(@chunkdir) rescue nil
  end

  test 'the path of chunk file was build with unique_id' do
    id = 'unique_nid'
    stub(Fluent::UniqueId).hex(anything) { id } # to fix chunk id
    c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
    expected = File.join(File.expand_path('./../', @chunk_path), "cio.#{id}.buf")
    assert_equal expected, c.path
  end

  sub_test_case '.initialize' do
    test 'create new chunk file' do
      meta = gen_metadata
      c = Fluent::Plugin::Buffer::ChunkioChunk.new(meta, @chunk_path, :create, chunk: @chunkio)

      assert File.exist?(c.path)
      assert c.empty?
      assert_equal meta, c.metadata
      assert_equal :unstaged, c.state
    end

    data(assume_mode: :assume, staged_mode: :staged, queued_mode: :queued)
    test 'load existing unstaged chunk file' do |mode|
      c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
      path = c.path

      newc = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, path, mode, chunk: @chunkio)
      assert File.exist?(path)
      assert_equal :unstaged, newc.state
    end

    data(assume_mode: :assume, staged_mode: :staged, queued_mode: :queued)
    test 'load existing staged chunk file with meta and other data' do |mode|
      m = gen_metadata(
        timekey: Time.parse('2016-04-07 17:40:00 +0900').to_i,
        tag: 'testing',
        variables: { k: 'v' },
      )
      c = Fluent::Plugin::Buffer::ChunkioChunk.new(m, @chunk_path, :create, chunk: @chunkio)
      path = c.path
      c.staged!

      newc = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, path, mode, chunk: @chunkio)
      assert File.exist?(path)
      assert_equal c.unique_id, newc.unique_id
      assert_equal c.modified_at, newc.modified_at
      assert_equal c.created_at, newc.created_at
      assert_equal c.size, newc.size
      assert_equal :staged, newc.state

      metadata = newc.metadata
      assert_equal m.timekey, metadata.timekey
      assert_equal m.tag, metadata.tag
      assert_equal m.variables, metadata.variables
    end

    data(assume_mode: :assume, staged_mode: :staged, queued_mode: :queued)
    test 'load existing enqueued chunk file with meta and other data' do |mode|
      m = gen_metadata(
        timekey: Time.parse('2016-04-07 17:40:00 +0900').to_i,
        tag: 'testing',
        variables: { k: 'v' },
      )
      c = Fluent::Plugin::Buffer::ChunkioChunk.new(m, @chunk_path, :create, chunk: @chunkio)
      path = c.path
      c.staged!
      c.enqueued!

      newc = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, path, mode, chunk: @chunkio)
      assert File.exist?(path)
      assert_equal c.unique_id, newc.unique_id
      assert_equal c.modified_at, newc.modified_at
      assert_equal c.created_at, newc.created_at
      assert_equal c.size, newc.size
      assert_equal :queued, newc.state

      metadata = newc.metadata
      assert_equal m.timekey, metadata.timekey
      assert_equal m.tag, metadata.tag
      assert_equal m.variables, metadata.variables
    end
  end

  sub_test_case '#append' do
    setup do
      @c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
    end

    teardown do
      @c && @c.purge rescue nil
    end

    test 'append data to chunk' do
      data = { 'k1' => 'v1', 'k2' => 'v2' }
      data2 = { 'k12' => 'v12', 'k22' => 'v22' }
      @c.append([data.to_json, data2.to_json].map { |e| "#{e}\n" })
      @c.commit

      ret = @c.read.split("\n")
      assert_equal 2, ret.size
      assert_equal data, JSON.parse(ret[0])
      assert_equal data2, JSON.parse(ret[1])

      @c.append([data.to_json, data2.to_json].map { |e| "#{e}\n" })
      @c.commit

      ret = @c.read.split("\n")
      assert_equal 4, ret.size
      assert_equal data, JSON.parse(ret[0])
      assert_equal data2, JSON.parse(ret[1])
      assert_equal data, JSON.parse(ret[2])
      assert_equal data2, JSON.parse(ret[3])
    end

    test 'appended data can be #rollback-ed until calling #commit' do
      data = { 'k1' => 'v1', 'k2' => 'v2' }
      data2 = { 'k12' => 'v12', 'k22' => 'v22' }
      @c.append([data.to_json, data2.to_json].map { |e| "#{e}\n" })
      @c.commit

      ret = @c.read.split("\n")
      assert_equal 2, ret.size
      assert_equal data, JSON.parse(ret[0])
      assert_equal data2, JSON.parse(ret[1])

      @c.append([data.to_json, data2.to_json].map { |e| "#{e}\n" })
      @c.rollback

      ret = @c.read.split("\n")
      assert_equal 2, ret.size
      assert_equal data, JSON.parse(ret[0])
      assert_equal data2, JSON.parse(ret[1])
    end
  end

  sub_test_case '#concat' do
    setup do
      @c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
    end

    teardown do
      @c && @c.purge rescue nil
    end

    test 'concat data to chunk' do
      data = { 'k1' => 'v1', 'k2' => 'v2' }
      data2 = { 'k12' => 'v12', 'k22' => 'v22' }
      @c.concat([data.to_json, data2.to_json].join("\n") + "\n", 2)
      @c.commit

      ret = @c.read.split("\n")
      assert_equal 2, ret.size
      assert_equal data, JSON.parse(ret[0])
      assert_equal data2, JSON.parse(ret[1])

      @c.concat([data.to_json, data2.to_json].join("\n") + "\n", 2)
      @c.commit

      ret = @c.read.split("\n")
      assert_equal 4, ret.size
      assert_equal data, JSON.parse(ret[0])
      assert_equal data2, JSON.parse(ret[1])
      assert_equal data, JSON.parse(ret[2])
      assert_equal data2, JSON.parse(ret[3])
    end

    test 'concated data can be #rollback-ed until calling #commit' do
      data = { 'k1' => 'v1', 'k2' => 'v2' }
      data2 = { 'k12' => 'v12', 'k22' => 'v22' }
      @c.concat([data.to_json, data2.to_json].join("\n") + "\n", 2)
      @c.commit

      ret = @c.read.split("\n")
      assert_equal 2, ret.size
      assert_equal data, JSON.parse(ret[0])
      assert_equal data2, JSON.parse(ret[1])

      @c.concat([data.to_json, data2.to_json].join("\n") + "\n", 2)
      @c.rollback

      ret = @c.read.split("\n")
      assert_equal 2, ret.size
      assert_equal data, JSON.parse(ret[0])
      assert_equal data2, JSON.parse(ret[1])
    end
  end

  sub_test_case '#read' do
    setup do
      @c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
    end

    teardown do
      @c && @c.purge rescue nil
    end

    test 'gets commited data multiple times' do
      ret = @c.read
      assert ret.empty?

      data = { 'k1' => 'v1', 'k2' => 'v2' }
      data2 = { 'k12' => 'v12', 'k22' => 'v22' }
      @c.concat([data.to_json, data2.to_json].join("\n") + "\n", 2)
      @c.commit

      ret = @c.read.split("\n")
      assert_equal 2, ret.size
      assert_equal data, JSON.parse(ret[0])
      assert_equal data2, JSON.parse(ret[1])
    end

    test 'return value is ascii-8bit' do
      data = 'utf-8 chars'.force_encoding('utf-8')
      @c.append([data])
      @c.commit

      assert_equal Encoding::ASCII_8BIT, @c.read.encoding
    end
  end

  sub_test_case '#size and #bytesize' do
    setup do
      @c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
    end

    teardown do
      @c && @c.purge rescue nil
    end

    test 'return size and bytesize' do
      @c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
      assert_equal 0, @c.size
      assert_equal 0, @c.bytesize
      data = { 'k1' => 'v1', 'k2' => 'v2' }.to_json

      @c.append([data + "\n"])
      assert_equal 1, @c.size
      assert_equal data.size + 1, @c.bytesize

      @c.append([data + "\n"])
      assert_equal 2, @c.size
      assert_equal (data.size + 1) * 2, @c.bytesize

      @c.rollback
      assert_equal 0, @c.size
      assert_equal 0, @c.bytesize

      @c.append([data + "\n"])
      assert_equal 1, @c.size
      assert_equal data.size + 1, @c.bytesize

      @c.commit
      assert_equal 1, @c.size
      assert_equal data.size + 1, @c.bytesize

      @c.rollback
      assert_equal 1, @c.size
      assert_equal data.size + 1, @c.bytesize
    end
  end

  sub_test_case '#enqueued!' do
    setup do
      @c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
    end

    teardown do
      @c && @c.purge rescue nil
    end

    test 'change state to enqueued' do
      assert_equal :unstaged, @c.state
      @c.staged!
      assert_equal :staged, @c.state
      @c.enqueued!
      assert_equal :queued, @c.state
    end

    test 'write metadata to chunk file' do
      @c.staged!
      @c.enqueued!
      c2 = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @c.path, :assume, chunk: @chunkio)
      assert_equal :queued, c2.state
    end

    test 'can not change state unless state is staged' do
      assert_equal :unstaged, @c.state
      @c.enqueued!
      assert_equal :unstaged, @c.state
    end
  end

  test '#open pass data to block' do
    c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
    data = { 'k1' => 'v1', 'k2' => 'v2' }.to_json
    c.append([data])
    c.commit
    c.open do |d|
      assert_equal data, d.read
    end
  end

  test 'calling #close closes file but not delete file' do
    c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
    assert File.exist?(c.path)
    c.close
    assert File.exist?(c.path)
  end

  test 'calling #purge unlink the chunk file' do
    c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio)
    assert File.exist?(c.path)
    c.purge
    assert_false File.exist?(c.path)
  end

  sub_test_case 'compressed buffer' do
    setup do
      @src = 'text data for compressing' * 5
      @gzipped_src = compress(@src)
      @c = Fluent::Plugin::Buffer::ChunkioChunk.new(gen_metadata, @chunk_path, :create, chunk: @chunkio, compress: :gzip)
    end

    teardown do
      @c && @c.purge rescue nil
    end

    test '#append with compress option writes  compressed data to chunk when compress is gzip' do
      @c.append([@src, @src], compress: :gzip)
      @c.commit

      # check chunk is compressed
      assert @c.read(compressed: :gzip).size < [@src, @src].join.size
      assert_equal (@src + @src), @c.read
    end

    test '#open passes io object having decompressed data to a block when compress is gzip' do
      @c.concat(@gzipped_src, @src.size)
      @c.commit

      decomressed_data = @c.open do |io|
        v = io.read
        assert_equal @src, v
        v
      end
      assert_equal @src, decomressed_data
    end

    test '#open with compressed option passes io object having decompressed data to a block when compress is gzip' do
      @c.concat(@gzipped_src, @src.size)
      @c.commit

      comressed_data = @c.open(compressed: :gzip) do |io|
        v = io.read
        assert_equal @gzipped_src, v
        v
      end
      assert_equal @gzipped_src, comressed_data
    end

    test '#write_to writes decompressed data when compress is gzip' do
      @c.concat(@gzipped_src, @src.size)
      @c.commit

      assert_equal @src, @c.read
      assert_equal @gzipped_src, @c.read(compressed: :gzip)

      io = StringIO.new
      @c.write_to(io)
      assert_equal @src, io.string
    end

    test '#write_to with compressed option writes compressed data when compress is gzip' do
      @c.concat(@gzipped_src, @src.size)
      @c.commit

      assert_equal @src, @c.read
      assert_equal @gzipped_src, @c.read(compressed: :gzip)

      io = StringIO.new
      io.set_encoding(Encoding::ASCII_8BIT)
      @c.write_to(io, compressed: :gzip)
      assert_equal @gzipped_src, io.string
    end
  end
end
