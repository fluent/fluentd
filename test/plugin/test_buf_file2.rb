require_relative '../helper'
require 'fluent/plugin/buf_file2'
require 'fluent/plugin/output'
require 'fluent/unique_id'
require 'fluent/system_config'

require 'msgpack'

module FluentPluginFileBufferTest
  class DummyOutputPlugin < Fluent::Plugin::Output
    Fluent::Plugin.register_output('buffer_file_test_output', self)
  end
end

class FileBufferTest < Test::Unit::TestCase
  def metadata(timekey: nil, tag: nil, variables: nil)
    Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
  end

  def write_metadata(path, chunk_id, metadata, records, ctime, mtime)
    metadata = {
      timekey: metadata.timekey, tag: metadata.tag, variables: metadata.variables,
      id: chunk_id,
      r: records,
      c: ctime,
      m: mtime,
    }
    File.open(path, 'w') do |f|
      f.write metadata.to_msgpack
    end
  end

  teardown do
    if @p
      @p.stop unless @p.stopped?
      @p.before_shutdown unless @p.before_shutdown?
      @p.shutdown unless @p.shutdown?
      @p.after_shutdown unless @p.after_shutdown?
      @p.close unless @p.closed?
      @p.terminate unless @p.terminated?
    end
  end

  sub_test_case 'non configured buffer plugin instance' do
    # tests for path
    test ''
  end

  sub_test_case 'buffer plugin configured only with path' do
    setup do
      @bufdir = File.expand_path('../../tmp/buffer_file', __FILE__)
      @bufpath = File.join(@bufdir, 'testbuf.*.log')
      FileUtils.rm_r @bufdir if File.exist?(@bufdir)

      Fluent::Test.setup
      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      @p = Fluent::Plugin::FileBuffer.new
      @p.owner = @d
      @p.configure(config_element('buffer', '', {'path' => @bufpath}))
      @p.start
    end

    test 'this is persistent plugin' do
      assert @p.persistent?
    end

    test '#start creates directory for buffer chunks' do
      plugin = Fluent::Plugin::FileBuffer.new
      plugin.owner = @d
      rand_num = rand(0..100)
      bufpath = File.join(File.expand_path("../../tmp/buffer_file_#{rand_num}", __FILE__), 'testbuf.*.log')
      bufdir = File.dirname(bufpath)

      FileUtils.rm_r bufdir if File.exist?(bufdir)
      assert !File.exist?(bufdir)

      plugin.configure(config_element('buffer', '', {'path' => bufpath}))
      assert !File.exist?(bufdir)

      plugin.start
      assert File.exist?(bufdir)
      assert{ File.stat(bufdir).mode.to_s(8).end_with?('755') }

      plugin.stop; plugin.before_shutdown; plugin.shutdown; plugin.after_shutdown; plugin.close; plugin.terminate
      FileUtils.rm_r bufdir
    end

    test '#start creates directory for buffer chunks with specified permission' do
      plugin = Fluent::Plugin::FileBuffer.new
      plugin.owner = @d
      rand_num = rand(0..100)
      bufpath = File.join(File.expand_path("../../tmp/buffer_file_#{rand_num}", __FILE__), 'testbuf.*.log')
      bufdir = File.dirname(bufpath)

      FileUtils.rm_r bufdir if File.exist?(bufdir)
      assert !File.exist?(bufdir)

      plugin.configure(config_element('buffer', '', {'path' => bufpath, 'dir_permission' => 0700}))
      assert !File.exist?(bufdir)

      plugin.start
      assert File.exist?(bufdir)
      assert{ File.stat(bufdir).mode.to_s(8).end_with?('700') }

      plugin.stop; plugin.before_shutdown; plugin.shutdown; plugin.after_shutdown; plugin.close; plugin.terminate
      FileUtils.rm_r bufdir
    end

    test '#start creates directory for buffer chunks with specified permission via system config' do
      sysconf = {'dir_permission' => '700'}
      Fluent::SystemConfig.overwrite_system_config(sysconf) do
        plugin = Fluent::Plugin::FileBuffer.new
        plugin.owner = @d
        rand_num = rand(0..100)
        bufpath = File.join(File.expand_path("../../tmp/buffer_file_#{rand_num}", __FILE__), 'testbuf.*.log')
        bufdir = File.dirname(bufpath)

        FileUtils.rm_r bufdir if File.exist?(bufdir)
        assert !File.exist?(bufdir)

        plugin.configure(config_element('buffer', '', {'path' => bufpath}))
        assert !File.exist?(bufdir)

        plugin.start
        assert File.exist?(bufdir)
        assert{ File.stat(bufdir).mode.to_s(8).end_with?('700') }

        plugin.stop; plugin.before_shutdown; plugin.shutdown; plugin.after_shutdown; plugin.close; plugin.terminate
        FileUtils.rm_r bufdir
      end
    end

    test '#generate_chunk generates blank file chunk on path from unique_id of metadata' do
      m1 = metadata()
      c1 = @p.generate_chunk(m1)
      assert c1.is_a? Fluent::Plugin::Buffer::FileChunk
      assert_equal m1, c1.metadata
      assert c1.empty?
      assert_equal :staged, c1.state
      assert_equal Fluent::Plugin::Buffer::FileChunk::FILE_PERMISSION, c1.permission
      assert_equal @bufpath.gsub('.*.', ".b#{Fluent::UniqueId.hex(c1.unique_id)}."), c1.path
      assert{ File.stat(c1.path).mode.to_s(8).end_with?('644') }

      m2 = metadata(timekey: event_time('2016-04-17 11:15:00 -0700').to_i)
      c2 = @p.generate_chunk(m2)
      assert c2.is_a? Fluent::Plugin::Buffer::FileChunk
      assert_equal m2, c2.metadata
      assert c2.empty?
      assert_equal :staged, c2.state
      assert_equal Fluent::Plugin::Buffer::FileChunk::FILE_PERMISSION, c2.permission
      assert_equal @bufpath.gsub('.*.', ".b#{Fluent::UniqueId.hex(c2.unique_id)}."), c2.path
      assert{ File.stat(c2.path).mode.to_s(8).end_with?('644') }
    end

    test '#generate_chunk generates blank file chunk with specified permission' do
      plugin = Fluent::Plugin::FileBuffer.new
      plugin.owner = @d
      rand_num = rand(0..100)
      bufpath = File.join(File.expand_path("../../tmp/buffer_file_#{rand_num}", __FILE__), 'testbuf.*.log')
      bufdir = File.dirname(bufpath)

      FileUtils.rm_r bufdir if File.exist?(bufdir)
      assert !File.exist?(bufdir)

      plugin.configure(config_element('buffer', '', {'path' => bufpath, 'file_permission' => 0600}))
      assert !File.exist?(bufdir)
      plugin.start

      m = metadata()
      c = plugin.generate_chunk(m)
      assert c.is_a? Fluent::Plugin::Buffer::FileChunk
      assert_equal m, c.metadata
      assert c.empty?
      assert_equal :staged, c.state
      assert_equal 0600, c.permission
      assert_equal bufpath.gsub('.*.', ".b#{Fluent::UniqueId.hex(c.unique_id)}."), c.path
      assert{ File.stat(c.path).mode.to_s(8).end_with?('600') }

      plugin.stop; plugin.before_shutdown; plugin.shutdown; plugin.after_shutdown; plugin.close; plugin.terminate
      FileUtils.rm_r bufdir
    end
  end


  sub_test_case 'there are no existing file chunks' do
    setup do
      @bufdir = File.expand_path('../../tmp/buffer_file', __FILE__)
      @bufpath = File.join(@bufdir, 'testbuf.*.log')
      FileUtils.rm_r @bufdir if File.exist?(@bufdir)

      Fluent::Test.setup
      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      @p = Fluent::Plugin::FileBuffer.new
      @p.owner = @d
      @p.configure(config_element('buffer', '', {'path' => @bufpath}))
      @p.start
    end

    test '#resume returns empty buffer state' do
      ary = @p.resume
      assert_equal({}, ary[0])
      assert_equal([], ary[1])
    end
  end

  sub_test_case 'there are some existing file chunks' do
    setup do
      @bufdir = File.expand_path('../../tmp/buffer_file', __FILE__)

      @c1id = Fluent::UniqueId.generate
      p1 = File.join(@bufdir, "etest.q#{Fluent::UniqueId.hex(@c1id)}.log")
      File.open(p1, 'w') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:58:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:58:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 13:58:22 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      write_metadata(
        p1 + '.meta', @c1id, metadata(timekey: event_time('2016-04-17 13:58:00 -0700').to_i),
        4, event_time('2016-04-17 13:58:00 -0700').to_i, event_time('2016-04-17 13:58:22 -0700').to_i
      )

      @c2id = Fluent::UniqueId.generate
      p2 = File.join(@bufdir, "etest.q#{Fluent::UniqueId.hex(@c2id)}.log")
      File.open(p2, 'w') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:59:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:59:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:59:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      write_metadata(
        p2 + '.meta', @c2id, metadata(timekey: event_time('2016-04-17 13:59:00 -0700').to_i),
        3, event_time('2016-04-17 13:59:00 -0700').to_i, event_time('2016-04-17 13:59:23 -0700').to_i
      )

      @c3id = Fluent::UniqueId.generate
      p3 = File.join(@bufdir, "etest.b#{Fluent::UniqueId.hex(@c3id)}.log")
      File.open(p3, 'w') do |f|
        f.write ["t1.test", event_time('2016-04-17 14:00:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 14:00:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 14:00:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 14:00:28 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      write_metadata(
        p3 + '.meta', @c3id, metadata(timekey: event_time('2016-04-17 14:00:00 -0700').to_i),
        4, event_time('2016-04-17 14:00:00 -0700').to_i, event_time('2016-04-17 14:00:28 -0700').to_i
      )

      @c4id = Fluent::UniqueId.generate
      p4 = File.join(@bufdir, "etest.b#{Fluent::UniqueId.hex(@c4id)}.log")
      File.open(p4, 'w') do |f|
        f.write ["t1.test", event_time('2016-04-17 14:01:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 14:01:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 14:01:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      write_metadata(
        p4 + '.meta', @c4id, metadata(timekey: event_time('2016-04-17 14:01:00 -0700').to_i),
        3, event_time('2016-04-17 14:01:00 -0700').to_i, event_time('2016-04-17 14:01:25 -0700').to_i
      )

      @bufpath = File.join(@bufdir, 'etest.*.log')

      Fluent::Test.setup
      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      @p = Fluent::Plugin::FileBuffer.new
      @p.owner = @d
      @p.configure(config_element('buffer', '', {'path' => @bufpath}))
      @p.start
    end

    teardown do
      Dir.glob(@bufpath).each do |path|
        File.unlink(path + '.meta') if File.exist?(path + '.meta')
        File.unlink(path) if File.exist?(path)
      end
    end

    test '#resume returns staged/queued chunks with metadata' do
      stage,queue = @p.resume
      assert_equal 2, stage.size
      assert_equal 2, queue.size

      m3 = metadata(timekey: event_time('2016-04-17 14:00:00 -0700').to_i)
      assert_equal @c3id, stage[m3].unique_id
      assert_equal 4, stage[m3].records
      assert_equal :staged, stage[m3].state

      m4 = metadata(timekey: event_time('2016-04-17 14:01:00 -0700').to_i)
      assert_equal @c4id, stage[m4].unique_id
      assert_equal 3, stage[m4].records
      assert_equal :staged, stage[m4].state
    end

    test '#resume returns queued chunks ordered by last modified time (FIFO)' do
      stage,queue = @p.resume
      assert_equal 2, stage.size
      assert_equal 2, queue.size

      assert{ queue[0].modified_at < queue[1].modified_at }

      assert_equal @c1id, queue[0].unique_id
      assert_equal :queued, queue[0].state
      assert_equal event_time('2016-04-17 13:58:00 -0700').to_i, queue[0].metadata.timekey
      assert_nil queue[0].metadata.tag
      assert_nil queue[0].metadata.variables
      assert_equal Time.parse('2016-04-17 13:58:00 -0700').localtime, queue[0].created_at
      assert_equal Time.parse('2016-04-17 13:58:22 -0700').localtime, queue[0].modified_at
      assert_equal 4, queue[0].records

      assert_equal @c2id, queue[1].unique_id
      assert_equal :queued, queue[1].state
      assert_equal event_time('2016-04-17 13:59:00 -0700').to_i, queue[1].metadata.timekey
      assert_nil queue[1].metadata.tag
      assert_nil queue[1].metadata.variables
      assert_equal Time.parse('2016-04-17 13:59:00 -0700').localtime, queue[1].created_at
      assert_equal Time.parse('2016-04-17 13:59:23 -0700').localtime, queue[1].modified_at
      assert_equal 3, queue[1].records
    end
  end

  sub_test_case 'there are some existing file chunks without metadata file' do
    test '#resume returns queued chunks for files without metadata'
  end
end
