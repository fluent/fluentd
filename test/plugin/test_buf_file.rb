require_relative '../helper'
require 'fluent/plugin/buf_file'
require 'fluent/plugin/output'
require 'fluent/unique_id'
require 'fluent/system_config'
require 'fluent/env'

require 'msgpack'

module FluentPluginFileBufferTest
  class DummyOutputPlugin < Fluent::Plugin::Output
    Fluent::Plugin.register_output('buffer_file_test_output', self)
    config_section :buffer do
      config_set_default :@type, 'file'
    end
    def multi_workers_ready?
      true
    end
    def write(chunk)
      # drop
    end
  end
end

class FileBufferTest < Test::Unit::TestCase
  def metadata(timekey: nil, tag: nil, variables: nil)
    Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
  end

  def write_metadata(path, chunk_id, metadata, size, ctime, mtime)
    metadata = {
      timekey: metadata.timekey, tag: metadata.tag, variables: metadata.variables,
      id: chunk_id,
      s: size,
      c: ctime,
      m: mtime,
    }
    File.open(path, 'wb') do |f|
      f.write metadata.to_msgpack
    end
  end

  sub_test_case 'non configured buffer plugin instance' do
    setup do
      Fluent::Test.setup

      @dir = File.expand_path('../../tmp/buffer_file_dir', __FILE__)
      FileUtils.rm_rf @dir
      FileUtils.mkdir_p @dir
    end

    test 'path should include * normally' do
      d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      p = Fluent::Plugin::FileBuffer.new
      p.owner = d
      p.configure(config_element('buffer', '', {'path' => File.join(@dir, 'buffer.*.file')}))
      assert_equal File.join(@dir, 'buffer.*.file'), p.path
    end

    test 'existing directory will be used with additional default file name' do
      d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      p = Fluent::Plugin::FileBuffer.new
      p.owner = d
      p.configure(config_element('buffer', '', {'path' => @dir}))
      assert_equal File.join(@dir, 'buffer.*.log'), p.path
    end

    test 'unexisting path without * handled as directory' do
      d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      p = Fluent::Plugin::FileBuffer.new
      p.owner = d
      p.configure(config_element('buffer', '', {'path' => File.join(@dir, 'buffer')}))
      assert_equal File.join(@dir, 'buffer', 'buffer.*.log'), p.path
    end
  end

  sub_test_case 'buffer configurations and workers' do
    setup do
      @bufdir = File.expand_path('../../tmp/buffer_file', __FILE__)
      FileUtils.rm_rf @bufdir
      Fluent::Test.setup

      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      @p = Fluent::Plugin::FileBuffer.new
      @p.owner = @d
    end

    test 'raise error if configured path is of existing file' do
      @bufpath = File.join(@bufdir, 'buf')
      FileUtils.mkdir_p @bufdir
      File.open(@bufpath, 'w'){|f| } # create and close the file
      assert File.exist?(@bufpath)
      assert File.file?(@bufpath)

      buf_conf = config_element('buffer', '', {'path' => @bufpath})
      assert_raise Fluent::ConfigError.new("Plugin 'file' does not support multi workers configuration (Fluent::Plugin::FileBuffer)") do
        Fluent::SystemConfig.overwrite_system_config('workers' => 4) do
          @d.configure(config_element('ROOT', '', {'@id' => 'dummy_output_with_buf'}, [buf_conf]))
        end
      end
    end

    test 'raise error if fluentd is configured to use file path pattern and multi workers' do
      @bufpath = File.join(@bufdir, 'testbuf.*.log')
      buf_conf = config_element('buffer', '', {'path' => @bufpath})
      assert_raise Fluent::ConfigError.new("Plugin 'file' does not support multi workers configuration (Fluent::Plugin::FileBuffer)") do
        Fluent::SystemConfig.overwrite_system_config('workers' => 4) do
          @d.configure(config_element('ROOT', '', {'@id' => 'dummy_output_with_buf'}, [buf_conf]))
        end
      end
    end

    test 'enables multi worker configuration with unexisting directory path' do
      assert_false File.exist?(@bufdir)
      buf_conf = config_element('buffer', '', {'path' => @bufdir})
      assert_nothing_raised do
        Fluent::SystemConfig.overwrite_system_config('root_dir' => @bufdir, 'workers' => 4) do
          @d.configure(config_element('ROOT', '', {}, [buf_conf]))
        end
      end
    end

    test 'enables multi worker configuration with existing directory path' do
      FileUtils.mkdir_p @bufdir
      buf_conf = config_element('buffer', '', {'path' => @bufdir})
      assert_nothing_raised do
        Fluent::SystemConfig.overwrite_system_config('root_dir' => @bufdir, 'workers' => 4) do
          @d.configure(config_element('ROOT', '', {}, [buf_conf]))
        end
      end
    end

    test 'enables multi worker configuration with root dir' do
      buf_conf = config_element('buffer', '')
      assert_nothing_raised do
        Fluent::SystemConfig.overwrite_system_config('root_dir' => @bufdir, 'workers' => 4) do
          @d.configure(config_element('ROOT', '', {'@id' => 'dummy_output_with_buf'}, [buf_conf]))
        end
      end
    end
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

    teardown do
      if @p
        @p.stop unless @p.stopped?
        @p.before_shutdown unless @p.before_shutdown?
        @p.shutdown unless @p.shutdown?
        @p.after_shutdown unless @p.after_shutdown?
        @p.close unless @p.closed?
        @p.terminate unless @p.terminated?
      end
      if @bufdir
        Dir.glob(File.join(@bufdir, '*')).each do |path|
          next if ['.', '..'].include?(File.basename(path))
          File.delete(path)
        end
      end
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
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

      plugin = Fluent::Plugin::FileBuffer.new
      plugin.owner = @d
      rand_num = rand(0..100)
      bufpath = File.join(File.expand_path("../../tmp/buffer_file_#{rand_num}", __FILE__), 'testbuf.*.log')
      bufdir = File.dirname(bufpath)

      FileUtils.rm_r bufdir if File.exist?(bufdir)
      assert !File.exist?(bufdir)

      plugin.configure(config_element('buffer', '', {'path' => bufpath, 'dir_permission' => '0700'}))
      assert !File.exist?(bufdir)

      plugin.start
      assert File.exist?(bufdir)
      assert{ File.stat(bufdir).mode.to_s(8).end_with?('700') }

      plugin.stop; plugin.before_shutdown; plugin.shutdown; plugin.after_shutdown; plugin.close; plugin.terminate
      FileUtils.rm_r bufdir
    end

    test '#start creates directory for buffer chunks with specified permission via system config' do
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

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
      assert_equal :unstaged, c1.state
      assert_equal Fluent::Plugin::Buffer::FileChunk::FILE_PERMISSION, c1.permission
      assert_equal @bufpath.gsub('.*.', ".b#{Fluent::UniqueId.hex(c1.unique_id)}."), c1.path
      assert{ File.stat(c1.path).mode.to_s(8).end_with?('644') }

      m2 = metadata(timekey: event_time('2016-04-17 11:15:00 -0700').to_i)
      c2 = @p.generate_chunk(m2)
      assert c2.is_a? Fluent::Plugin::Buffer::FileChunk
      assert_equal m2, c2.metadata
      assert c2.empty?
      assert_equal :unstaged, c2.state
      assert_equal Fluent::Plugin::Buffer::FileChunk::FILE_PERMISSION, c2.permission
      assert_equal @bufpath.gsub('.*.', ".b#{Fluent::UniqueId.hex(c2.unique_id)}."), c2.path
      assert{ File.stat(c2.path).mode.to_s(8).end_with?('644') }

      c1.purge
      c2.purge
    end

    test '#generate_chunk generates blank file chunk with specified permission' do
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

      plugin = Fluent::Plugin::FileBuffer.new
      plugin.owner = @d
      rand_num = rand(0..100)
      bufpath = File.join(File.expand_path("../../tmp/buffer_file_#{rand_num}", __FILE__), 'testbuf.*.log')
      bufdir = File.dirname(bufpath)

      FileUtils.rm_r bufdir if File.exist?(bufdir)
      assert !File.exist?(bufdir)

      plugin.configure(config_element('buffer', '', {'path' => bufpath, 'file_permission' => '0600'}))
      assert !File.exist?(bufdir)
      plugin.start

      m = metadata()
      c = plugin.generate_chunk(m)
      assert c.is_a? Fluent::Plugin::Buffer::FileChunk
      assert_equal m, c.metadata
      assert c.empty?
      assert_equal :unstaged, c.state
      assert_equal 0600, c.permission
      assert_equal bufpath.gsub('.*.', ".b#{Fluent::UniqueId.hex(c.unique_id)}."), c.path
      assert{ File.stat(c.path).mode.to_s(8).end_with?('600') }

      c.purge

      plugin.stop; plugin.before_shutdown; plugin.shutdown; plugin.after_shutdown; plugin.close; plugin.terminate
      FileUtils.rm_r bufdir
    end
  end

  sub_test_case 'configured with system root directory and plugin @id' do
    setup do
      @root_dir = File.expand_path('../../tmp/buffer_file_root', __FILE__)
      FileUtils.rm_rf @root_dir

      Fluent::Test.setup
      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      @p = Fluent::Plugin::FileBuffer.new
      @p.owner = @d
      Fluent::SystemConfig.overwrite_system_config('root_dir' => @root_dir) do
        @d.configure(config_element('ROOT', '', {'@id' => 'dummy_output_with_buf'}))
        @p.configure(config_element('buffer', ''))
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

    test '#start creates directory for buffer chunks' do
      expected_buffer_path = File.join(@root_dir, 'worker0', 'dummy_output_with_buf', 'buffer', 'buffer.*.log')
      expected_buffer_dir = File.dirname(expected_buffer_path)
      assert_equal expected_buffer_path, @p.path
      assert_false Dir.exist?(expected_buffer_dir)

      @p.start

      assert Dir.exist?(expected_buffer_dir)
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
    teardown do
      if @p
        @p.stop unless @p.stopped?
        @p.before_shutdown unless @p.before_shutdown?
        @p.shutdown unless @p.shutdown?
        @p.after_shutdown unless @p.after_shutdown?
        @p.close unless @p.closed?
        @p.terminate unless @p.terminated?
      end
      if @bufdir
        Dir.glob(File.join(@bufdir, '*')).each do |path|
          next if ['.', '..'].include?(File.basename(path))
          File.delete(path)
        end
      end
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
      FileUtils.mkdir_p @bufdir unless File.exist?(@bufdir)

      @c1id = Fluent::UniqueId.generate
      p1 = File.join(@bufdir, "etest.q#{Fluent::UniqueId.hex(@c1id)}.log")
      File.open(p1, 'wb') do |f|
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
      File.open(p2, 'wb') do |f|
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
      File.open(p3, 'wb') do |f|
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
      File.open(p4, 'wb') do |f|
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
      if @p
        @p.stop unless @p.stopped?
        @p.before_shutdown unless @p.before_shutdown?
        @p.shutdown unless @p.shutdown?
        @p.after_shutdown unless @p.after_shutdown?
        @p.close unless @p.closed?
        @p.terminate unless @p.terminated?
      end
      if @bufdir
        Dir.glob(File.join(@bufdir, '*')).each do |path|
          next if ['.', '..'].include?(File.basename(path))
          File.delete(path)
        end
      end
    end

    test '#resume returns staged/queued chunks with metadata' do
      assert_equal 2, @p.stage.size
      assert_equal 2, @p.queue.size

      stage = @p.stage

      m3 = metadata(timekey: event_time('2016-04-17 14:00:00 -0700').to_i)
      assert_equal @c3id, stage[m3].unique_id
      assert_equal 4, stage[m3].size
      assert_equal :staged, stage[m3].state

      m4 = metadata(timekey: event_time('2016-04-17 14:01:00 -0700').to_i)
      assert_equal @c4id, stage[m4].unique_id
      assert_equal 3, stage[m4].size
      assert_equal :staged, stage[m4].state
    end

    test '#resume returns queued chunks ordered by last modified time (FIFO)' do
      assert_equal 2, @p.stage.size
      assert_equal 2, @p.queue.size

      queue = @p.queue

      assert{ queue[0].modified_at < queue[1].modified_at }

      assert_equal @c1id, queue[0].unique_id
      assert_equal :queued, queue[0].state
      assert_equal event_time('2016-04-17 13:58:00 -0700').to_i, queue[0].metadata.timekey
      assert_nil queue[0].metadata.tag
      assert_nil queue[0].metadata.variables
      assert_equal Time.parse('2016-04-17 13:58:00 -0700').localtime, queue[0].created_at
      assert_equal Time.parse('2016-04-17 13:58:22 -0700').localtime, queue[0].modified_at
      assert_equal 4, queue[0].size

      assert_equal @c2id, queue[1].unique_id
      assert_equal :queued, queue[1].state
      assert_equal event_time('2016-04-17 13:59:00 -0700').to_i, queue[1].metadata.timekey
      assert_nil queue[1].metadata.tag
      assert_nil queue[1].metadata.variables
      assert_equal Time.parse('2016-04-17 13:59:00 -0700').localtime, queue[1].created_at
      assert_equal Time.parse('2016-04-17 13:59:23 -0700').localtime, queue[1].modified_at
      assert_equal 3, queue[1].size
    end
  end

  sub_test_case 'there are some existing file chunks, both in specified path and per-worker directory under specified path, configured as multi workers' do
    setup do
      @bufdir = File.expand_path('../../tmp/buffer_file/path', __FILE__)
      @worker0_dir = File.join(@bufdir, "worker0")
      @worker1_dir = File.join(@bufdir, "worker1")
      FileUtils.rm_rf @bufdir
      FileUtils.mkdir_p @worker0_dir
      FileUtils.mkdir_p @worker1_dir

      @bufdir_chunk_1 = Fluent::UniqueId.generate
      bc1 = File.join(@bufdir, "buffer.q#{Fluent::UniqueId.hex(@bufdir_chunk_1)}.log")
      File.open(bc1, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:58:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:58:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 13:58:22 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      write_metadata(
        bc1 + '.meta', @bufdir_chunk_1, metadata(timekey: event_time('2016-04-17 13:58:00 -0700').to_i),
        4, event_time('2016-04-17 13:58:00 -0700').to_i, event_time('2016-04-17 13:58:22 -0700').to_i
      )

      @bufdir_chunk_2 = Fluent::UniqueId.generate
      bc2 = File.join(@bufdir, "buffer.q#{Fluent::UniqueId.hex(@bufdir_chunk_2)}.log")
      File.open(bc2, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:58:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:58:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 13:58:22 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      write_metadata(
        bc2 + '.meta', @bufdir_chunk_2, metadata(timekey: event_time('2016-04-17 13:58:00 -0700').to_i),
        4, event_time('2016-04-17 13:58:00 -0700').to_i, event_time('2016-04-17 13:58:22 -0700').to_i
      )

      @worker_dir_chunk_1 = Fluent::UniqueId.generate
      wc0_1 = File.join(@worker0_dir, "buffer.q#{Fluent::UniqueId.hex(@worker_dir_chunk_1)}.log")
      wc1_1 = File.join(@worker1_dir, "buffer.q#{Fluent::UniqueId.hex(@worker_dir_chunk_1)}.log")
      [wc0_1, wc1_1].each do |chunk_path|
        File.open(chunk_path, 'wb') do |f|
          f.write ["t1.test", event_time('2016-04-17 13:59:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t2.test", event_time('2016-04-17 13:59:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t3.test", event_time('2016-04-17 13:59:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        end
        write_metadata(
          chunk_path + '.meta', @worker_dir_chunk_1, metadata(timekey: event_time('2016-04-17 13:59:00 -0700').to_i),
          3, event_time('2016-04-17 13:59:00 -0700').to_i, event_time('2016-04-17 13:59:23 -0700').to_i
        )
      end

      @worker_dir_chunk_2 = Fluent::UniqueId.generate
      wc0_2 = File.join(@worker0_dir, "buffer.b#{Fluent::UniqueId.hex(@worker_dir_chunk_2)}.log")
      wc1_2 = File.join(@worker1_dir, "buffer.b#{Fluent::UniqueId.hex(@worker_dir_chunk_2)}.log")
      [wc0_2, wc1_2].each do |chunk_path|
        File.open(chunk_path, 'wb') do |f|
          f.write ["t1.test", event_time('2016-04-17 14:00:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t2.test", event_time('2016-04-17 14:00:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t3.test", event_time('2016-04-17 14:00:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t4.test", event_time('2016-04-17 14:00:28 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        end
        write_metadata(
          chunk_path + '.meta', @worker_dir_chunk_2, metadata(timekey: event_time('2016-04-17 14:00:00 -0700').to_i),
          4, event_time('2016-04-17 14:00:00 -0700').to_i, event_time('2016-04-17 14:00:28 -0700').to_i
        )
      end

      @worker_dir_chunk_3 = Fluent::UniqueId.generate
      wc0_3 = File.join(@worker0_dir, "buffer.b#{Fluent::UniqueId.hex(@worker_dir_chunk_3)}.log")
      wc1_3 = File.join(@worker1_dir, "buffer.b#{Fluent::UniqueId.hex(@worker_dir_chunk_3)}.log")
      [wc0_3, wc1_3].each do |chunk_path|
        File.open(chunk_path, 'wb') do |f|
          f.write ["t1.test", event_time('2016-04-17 14:01:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t2.test", event_time('2016-04-17 14:01:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t3.test", event_time('2016-04-17 14:01:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        end
        write_metadata(
          chunk_path + '.meta', @worker_dir_chunk_3, metadata(timekey: event_time('2016-04-17 14:01:00 -0700').to_i),
          3, event_time('2016-04-17 14:01:00 -0700').to_i, event_time('2016-04-17 14:01:25 -0700').to_i
        )
      end

      Fluent::Test.setup
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

    test 'worker(id=0) #resume returns staged/queued chunks with metadata, not only in worker dir, including the directory specified by path' do
      ENV['SERVERENGINE_WORKER_ID'] = '0'

      buf_conf = config_element('buffer', '', {'path' => @bufdir})
      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      with_worker_config(workers: 2, worker_id: 0) do
        @d.configure(config_element('output', '', {}, [buf_conf]))
      end

      @d.start
      @p = @d.buffer

      assert_equal 2, @p.stage.size
      assert_equal 3, @p.queue.size

      stage = @p.stage

      m1 = metadata(timekey: event_time('2016-04-17 14:00:00 -0700').to_i)
      assert_equal @worker_dir_chunk_2, stage[m1].unique_id
      assert_equal 4, stage[m1].size
      assert_equal :staged, stage[m1].state

      m2 = metadata(timekey: event_time('2016-04-17 14:01:00 -0700').to_i)
      assert_equal @worker_dir_chunk_3, stage[m2].unique_id
      assert_equal 3, stage[m2].size
      assert_equal :staged, stage[m2].state

      queue = @p.queue

      assert_equal [@bufdir_chunk_1, @bufdir_chunk_2, @worker_dir_chunk_1].sort, queue.map(&:unique_id).sort
      assert_equal [3, 4, 4], queue.map(&:size).sort
      assert_equal [:queued, :queued, :queued], queue.map(&:state)
    end

    test 'worker(id=1) #resume returns staged/queued chunks with metadata, only in worker dir' do
      buf_conf = config_element('buffer', '', {'path' => @bufdir})
      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      with_worker_config(workers: 2, worker_id: 1) do
        @d.configure(config_element('output', '', {}, [buf_conf]))
      end

      @d.start
      @p = @d.buffer

      assert_equal 2, @p.stage.size
      assert_equal 1, @p.queue.size

      stage = @p.stage

      m1 = metadata(timekey: event_time('2016-04-17 14:00:00 -0700').to_i)
      assert_equal @worker_dir_chunk_2, stage[m1].unique_id
      assert_equal 4, stage[m1].size
      assert_equal :staged, stage[m1].state

      m2 = metadata(timekey: event_time('2016-04-17 14:01:00 -0700').to_i)
      assert_equal @worker_dir_chunk_3, stage[m2].unique_id
      assert_equal 3, stage[m2].size
      assert_equal :staged, stage[m2].state

      queue = @p.queue

      assert_equal @worker_dir_chunk_1, queue[0].unique_id
      assert_equal 3, queue[0].size
      assert_equal :queued, queue[0].state
    end
  end

  sub_test_case 'there are some existing file chunks without metadata file' do
    setup do
      @bufdir = File.expand_path('../../tmp/buffer_file', __FILE__)

      @c1id = Fluent::UniqueId.generate
      p1 = File.join(@bufdir, "etest.201604171358.q#{Fluent::UniqueId.hex(@c1id)}.log")
      File.open(p1, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:58:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:58:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 13:58:22 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      FileUtils.touch(p1, mtime: Time.parse('2016-04-17 13:58:28 -0700'))

      @c2id = Fluent::UniqueId.generate
      p2 = File.join(@bufdir, "etest.201604171359.q#{Fluent::UniqueId.hex(@c2id)}.log")
      File.open(p2, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:59:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:59:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:59:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      FileUtils.touch(p2, mtime: Time.parse('2016-04-17 13:59:30 -0700'))

      @c3id = Fluent::UniqueId.generate
      p3 = File.join(@bufdir, "etest.201604171400.b#{Fluent::UniqueId.hex(@c3id)}.log")
      File.open(p3, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 14:00:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 14:00:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 14:00:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 14:00:28 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      FileUtils.touch(p3, mtime: Time.parse('2016-04-17 14:00:29 -0700'))

      @c4id = Fluent::UniqueId.generate
      p4 = File.join(@bufdir, "etest.201604171401.b#{Fluent::UniqueId.hex(@c4id)}.log")
      File.open(p4, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 14:01:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 14:01:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 14:01:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      FileUtils.touch(p4, mtime: Time.parse('2016-04-17 14:01:22 -0700'))

      @bufpath = File.join(@bufdir, 'etest.*.log')

      Fluent::Test.setup
      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      @p = Fluent::Plugin::FileBuffer.new
      @p.owner = @d
      @p.configure(config_element('buffer', '', {'path' => @bufpath}))
      @p.start
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
      if @bufdir
        Dir.glob(File.join(@bufdir, '*')).each do |path|
          next if ['.', '..'].include?(File.basename(path))
          File.delete(path)
        end
      end
    end

    test '#resume returns queued chunks for files without metadata' do
      assert_equal 0, @p.stage.size
      assert_equal 4, @p.queue.size

      queue = @p.queue

      m = metadata()

      assert_equal @c1id, queue[0].unique_id
      assert_equal m, queue[0].metadata
      assert_equal 0, queue[0].size
      assert_equal :queued, queue[0].state
      assert_equal Time.parse('2016-04-17 13:58:28 -0700'), queue[0].modified_at

      assert_equal @c2id, queue[1].unique_id
      assert_equal m, queue[1].metadata
      assert_equal 0, queue[1].size
      assert_equal :queued, queue[1].state
      assert_equal Time.parse('2016-04-17 13:59:30 -0700'), queue[1].modified_at

      assert_equal @c3id, queue[2].unique_id
      assert_equal m, queue[2].metadata
      assert_equal 0, queue[2].size
      assert_equal :queued, queue[2].state
      assert_equal Time.parse('2016-04-17 14:00:29 -0700'), queue[2].modified_at

      assert_equal @c4id, queue[3].unique_id
      assert_equal m, queue[3].metadata
      assert_equal 0, queue[3].size
      assert_equal :queued, queue[3].state
      assert_equal Time.parse('2016-04-17 14:01:22 -0700'), queue[3].modified_at
    end
  end

  sub_test_case 'there are some non-buffer chunk files, with a path without buffer chunk ids' do
    setup do
      @bufdir = File.expand_path('../../tmp/buffer_file', __FILE__)

      FileUtils.rm_rf @bufdir
      FileUtils.mkdir_p @bufdir

      @c1id = Fluent::UniqueId.generate
      p1 = File.join(@bufdir, "etest.201604171358.q#{Fluent::UniqueId.hex(@c1id)}.log")
      File.open(p1, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:58:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:58:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 13:58:22 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      FileUtils.touch(p1, mtime: Time.parse('2016-04-17 13:58:28 -0700'))

      @not_chunk = File.join(@bufdir, 'etest.20160416.log')
      File.open(@not_chunk, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-16 23:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-16 23:58:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-16 23:58:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-16 23:58:22 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      FileUtils.touch(@not_chunk, mtime: Time.parse('2016-04-17 00:00:00 -0700'))

      @bufpath = File.join(@bufdir, 'etest.*.log')

      Fluent::Test.setup
      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      @p = Fluent::Plugin::FileBuffer.new
      @p.owner = @d
      @p.configure(config_element('buffer', '', {'path' => @bufpath}))
      @p.start
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
      if @bufdir
        Dir.glob(File.join(@bufdir, '*')).each do |path|
          next if ['.', '..'].include?(File.basename(path))
          File.delete(path)
        end
      end
    end

    test '#resume returns queued chunks for files without metadata, while ignoring non-chunk looking files' do
      assert_equal 0, @p.stage.size
      assert_equal 1, @p.queue.size

      queue = @p.queue

      m = metadata()

      assert_equal @c1id, queue[0].unique_id
      assert_equal m, queue[0].metadata
      assert_equal 0, queue[0].size
      assert_equal :queued, queue[0].state
      assert_equal Time.parse('2016-04-17 13:58:28 -0700'), queue[0].modified_at

      assert File.exist?(@not_chunk)
    end
  end

  sub_test_case 'there are existing broken file chunks' do
    setup do
      @bufdir = File.expand_path('../../tmp/broken_buffer_file', __FILE__)
      FileUtils.mkdir_p @bufdir unless File.exist?(@bufdir)
      @bufpath = File.join(@bufdir, 'broken_test.*.log')

      Fluent::Test.setup
      @d = FluentPluginFileBufferTest::DummyOutputPlugin.new
      @p = Fluent::Plugin::FileBuffer.new
      @p.owner = @d
      @p.configure(config_element('buffer', '', {'path' => @bufpath}))
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
      if @bufdir
        Dir.glob(File.join(@bufdir, '*')).each do |path|
          next if ['.', '..'].include?(File.basename(path))
          File.delete(path)
        end
      end
    end

    def create_first_chunk(mode)
      cid = Fluent::UniqueId.generate
      path = File.join(@bufdir, "broken_test.#{mode}#{Fluent::UniqueId.hex(cid)}.log")
      File.open(path, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 14:00:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 14:00:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 14:00:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 14:00:28 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      write_metadata(
        path + '.meta', cid, metadata(timekey: event_time('2016-04-17 14:00:00 -0700').to_i),
        4, event_time('2016-04-17 14:00:00 -0700').to_i, event_time('2016-04-17 14:00:28 -0700').to_i
      )

      return cid, path
    end

    def create_second_chunk(mode)
      cid = Fluent::UniqueId.generate
      path = File.join(@bufdir, "broken_test.#{mode}#{Fluent::UniqueId.hex(cid)}.log")
      File.open(path, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 14:01:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 14:01:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 14:01:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      write_metadata(
        path + '.meta', cid, metadata(timekey: event_time('2016-04-17 14:01:00 -0700').to_i),
        3, event_time('2016-04-17 14:01:00 -0700').to_i, event_time('2016-04-17 14:01:25 -0700').to_i
      )

      return cid, path
    end

    def compare_staged_chunk(staged, id, time, num, mode)
      assert_equal 1, staged.size
      m = metadata(timekey: event_time(time).to_i)
      assert_equal id, staged[m].unique_id
      assert_equal num, staged[m].size
      assert_equal mode, staged[m].state
    end

     def compare_queued_chunk(queued, id, num, mode)
      assert_equal 1, queued.size
      assert_equal id, queued[0].unique_id
      assert_equal num, queued[0].size
      assert_equal mode, queued[0].state
    end

    def compare_log(plugin, msg)
      logs = plugin.log.out.logs
      assert { logs.any? { |log| log.include?(msg) } }
    end

    test '#resume ignores staged empty chunk' do
      _, p1 = create_first_chunk('b')
      File.open(p1, 'wb') { |f| } # create staged empty chunk file
      c2id, _ = create_second_chunk('b')

      @p.start
      compare_staged_chunk(@p.stage, c2id, '2016-04-17 14:01:00 -0700', 3, :staged)
      compare_log(@p, 'staged file chunk is empty')
    end

    test '#resume ignores staged broken metadata' do
      c1id, _ = create_first_chunk('b')
      _, p2 = create_second_chunk('b')
      File.open(p2 + '.meta', 'wb') { |f| f.write("\0" * 70) } # create staged broken meta file

      @p.start
      compare_staged_chunk(@p.stage, c1id, '2016-04-17 14:00:00 -0700', 4, :staged)
      compare_log(@p, 'staged meta file is broken')
    end

    test '#resume ignores enqueued empty chunk' do
      _, p1 = create_first_chunk('q')
      File.open(p1, 'wb') { |f| } # create enqueued empty chunk file
      c2id, _ = create_second_chunk('q')

      @p.start
      compare_queued_chunk(@p.queue, c2id, 3, :queued)
      compare_log(@p, 'enqueued file chunk is empty')
    end

    test '#resume ignores enqueued broken metadata' do
      c1id, _ = create_first_chunk('q')
      _, p2 = create_second_chunk('q')
      File.open(p2 + '.meta', 'wb') { |f| f.write("\0" * 70) } # create enqueued broken meta file

      @p.start
      compare_queued_chunk(@p.queue, c1id, 4, :queued)
      compare_log(@p, 'enqueued meta file is broken')
    end
  end
end
