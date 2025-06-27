require_relative '../helper'
require 'fluent/plugin/buf_file_single'
require 'fluent/plugin/output'
require 'fluent/unique_id'
require 'fluent/system_config'
require 'fluent/env'
require 'fluent/test/driver/output'

require 'msgpack'

module FluentPluginFileSingleBufferTest
  class DummyOutputPlugin < Fluent::Plugin::Output
    Fluent::Plugin.register_output('buf_file_single_test', self)
    config_section :buffer do
      config_set_default :@type, 'file_single'
    end
    def multi_workers_ready?
      true
    end
    def write(chunk)
      # drop
    end
  end

  class DummyOutputMPPlugin < Fluent::Plugin::Output
    Fluent::Plugin.register_output('buf_file_single_mp_test', self)
    config_section :buffer do
      config_set_default :@type, 'file_single'
    end
    def formatted_to_msgpack_binary?
      true
    end
    def multi_workers_ready?
      true
    end
    def write(chunk)
      # drop
    end
  end

  class DummyErrorOutputPlugin < DummyOutputPlugin
    def register_write(&block)
      instance_variable_set(:@write, block)
    end

    def initialize
      super
      @should_fail_writing = true
      @write = nil
    end

    def recover
      @should_fail_writing = false
    end

    def write(chunk)
      if @should_fail_writing
        raise "failed writing chunk"
      else
        @write ? @write.call(chunk) : nil
      end
    end

    def format(tag, time, record)
      [tag, time.to_i, record].to_json + "\n"
    end
  end
end

class FileSingleBufferTest < Test::Unit::TestCase
  def metadata(timekey: nil, tag: 'testing', variables: nil)
    Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
  end

  PATH = File.expand_path('../../tmp/buffer_file_single_dir', __FILE__)
  TAG_CONF = %[
    <buffer tag>
      @type file_single
      path #{PATH}
    </buffer>
  ]
  FIELD_CONF = %[
    <buffer k>
      @type file_single
      path #{PATH}
    </buffer>
  ]

  setup do
    Fluent::Test.setup

    @d = nil
    @bufdir = PATH
    FileUtils.rm_rf(@bufdir) rescue nil
    FileUtils.mkdir_p(@bufdir)
  end

  teardown do
    FileUtils.rm_rf(@bufdir) rescue nil
  end

  def create_driver(conf = TAG_CONF, klass = FluentPluginFileSingleBufferTest::DummyOutputPlugin)
    Fluent::Test::Driver::Output.new(klass).configure(conf)
  end

  sub_test_case 'configuration' do
    test 'path has "fsb" prefix and "buf" suffix by default' do
      @d = create_driver
      p = @d.instance.buffer
      assert_equal File.join(@bufdir, 'fsb.*.buf'), p.path
    end

    data('text based chunk' => [FluentPluginFileSingleBufferTest::DummyOutputPlugin, :text],
         'msgpack based chunk' => [FluentPluginFileSingleBufferTest::DummyOutputMPPlugin, :msgpack])
    test 'detect chunk_format' do |param|
      klass, expected = param
      @d = create_driver(TAG_CONF, klass)
      p = @d.instance.buffer
      assert_equal expected, p.chunk_format
    end

    test '"prefix.*.suffix" path will be replaced with default' do
      @d = create_driver(%[
        <buffer tag>
          @type file_single
          path #{@bufdir}/foo.*.bar
        </buffer>
      ])
      p = @d.instance.buffer
      assert_equal File.join(@bufdir, 'fsb.*.buf'), p.path
    end
  end

  sub_test_case 'buffer configurations and workers' do
    setup do
      @d = FluentPluginFileSingleBufferTest::DummyOutputPlugin.new
    end

    test 'enables multi worker configuration with unexisting directory path' do
      FileUtils.rm_rf(@bufdir)
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

  test 'raise config error when using same file path' do
    d = FluentPluginFileSingleBufferTest::DummyOutputPlugin.new
    d2 = FluentPluginFileSingleBufferTest::DummyOutputPlugin.new
    Fluent::SystemConfig.overwrite_system_config({}) do
      d.configure(config_element('ROOT', '', {}, [config_element('buffer', '', { 'path' => File.join(PATH, 'foo.*.bar') })]))
    end

    any_instance_of(Fluent::Plugin::FileSingleBuffer) do |klass|
      stub(klass).called_in_test? { false }
    end

    err = assert_raise(Fluent::ConfigError) do
      Fluent::SystemConfig.overwrite_system_config({}) do
        d2.configure(config_element('ROOT', '', {}, [config_element('buffer', '', { 'path' => PATH })]))
      end
    end
    assert_match(/plugin already uses same buffer path/, err.message)
  end

  sub_test_case 'buffer plugin configured only with path' do
    setup do
      @bufpath = File.join(@bufdir, 'testbuf.*.buf')
      FileUtils.rm_rf(@bufdir) if File.exist?(@bufdir)

      @d = create_driver
      @p = @d.instance.buffer
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

    test 'this is persistent plugin' do
      assert @p.persistent?
    end

    test '#start creates directory for buffer chunks' do
      @d = create_driver
      @p = @d.instance.buffer

      FileUtils.rm_rf(@bufdir) if File.exist?(@bufdir)
      assert !File.exist?(@bufdir)

      @p.start
      assert File.exist?(@bufdir)
      assert { File.stat(@bufdir).mode.to_s(8).end_with?('755') }
    end

    test '#start creates directory for buffer chunks with specified permission' do
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

      @d = create_driver(%[
        <buffer tag>
          @type file_single
          path #{PATH}
          dir_permission 700
        </buffer>
      ])
      @p = @d.instance.buffer

      FileUtils.rm_rf(@bufdir) if File.exist?(@bufdir)
      assert !File.exist?(@bufdir)

      @p.start
      assert File.exist?(@bufdir)
      assert { File.stat(@bufdir).mode.to_s(8).end_with?('700') }
    end

    test '#start creates directory for buffer chunks with specified permission via system config' do
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

      sysconf = {'dir_permission' => '700'}
      Fluent::SystemConfig.overwrite_system_config(sysconf) do
        @d = create_driver
        @p = @d.instance.buffer

        FileUtils.rm_r @bufdir if File.exist?(@bufdir)
        assert !File.exist?(@bufdir)

        @p.start
        assert File.exist?(@bufdir)
        assert { File.stat(@bufdir).mode.to_s(8).end_with?('700') }
      end
    end

    test '#generate_chunk generates blank file chunk on path with unique_id and tag' do
      FileUtils.mkdir_p(@bufdir) unless File.exist?(@bufdir)

      m1 = metadata()
      c1 = @p.generate_chunk(m1)
      assert c1.is_a? Fluent::Plugin::Buffer::FileSingleChunk
      assert_equal m1, c1.metadata
      assert c1.empty?
      assert_equal :unstaged, c1.state
      assert_equal Fluent::DEFAULT_FILE_PERMISSION, c1.permission
      assert_equal File.join(@bufdir, "fsb.testing.b#{Fluent::UniqueId.hex(c1.unique_id)}.buf"), c1.path
      assert{ File.stat(c1.path).mode.to_s(8).end_with?('644') }

      c1.purge
    end

    test '#generate_chunk generates blank file chunk on path with unique_id and field key' do
      FileUtils.mkdir_p(@bufdir) unless File.exist?(@bufdir)

      @d = create_driver(FIELD_CONF)
      @p = @d.instance.buffer

      m1 = metadata(tag: nil, variables: {:k => 'foo_bar'})
      c1 = @p.generate_chunk(m1)
      assert c1.is_a? Fluent::Plugin::Buffer::FileSingleChunk
      assert_equal m1, c1.metadata
      assert c1.empty?
      assert_equal :unstaged, c1.state
      assert_equal Fluent::DEFAULT_FILE_PERMISSION, c1.permission
      assert_equal File.join(@bufdir, "fsb.foo_bar.b#{Fluent::UniqueId.hex(c1.unique_id)}.buf"), c1.path

      c1.purge
    end

    test '#generate_chunk generates blank file chunk with specified permission' do
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

      @d = create_driver(%[
        <buffer tag>
          @type file_single
          path #{PATH}
          file_permission 600
        </buffer>
      ])
      @p = @d.instance.buffer

      FileUtils.rm_r @bufdir if File.exist?(@bufdir)
      assert !File.exist?(@bufdir)

      @p.start

      m = metadata()
      c = @p.generate_chunk(m)
      assert c.is_a? Fluent::Plugin::Buffer::FileSingleChunk
      assert_equal m, c.metadata
      assert c.empty?
      assert_equal :unstaged, c.state
      assert_equal 0600, c.permission
      assert_equal File.join(@bufdir, "fsb.testing.b#{Fluent::UniqueId.hex(c.unique_id)}.buf"), c.path
      assert{ File.stat(c.path).mode.to_s(8).end_with?('600') }

      c.purge
    end

    test '#generate_chunk generates blank file chunk with specified permission with system_config' do
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

      @d = create_driver(%[
        <buffer tag>
          @type file_single
          path #{PATH}
        </buffer>
      ])
      @p = @d.instance.buffer

      FileUtils.rm_r @bufdir if File.exist?(@bufdir)
      assert !File.exist?(@bufdir)

      @p.start

      m = metadata()
      c = nil
      Fluent::SystemConfig.overwrite_system_config("file_permission" => "700") do
        c = @p.generate_chunk(m)
      end
      assert c.is_a? Fluent::Plugin::Buffer::FileSingleChunk
      assert_equal m, c.metadata
      assert c.empty?
      assert_equal :unstaged, c.state
      assert_equal 0700, c.permission
      assert_equal File.join(@bufdir, "fsb.testing.b#{Fluent::UniqueId.hex(c.unique_id)}.buf"), c.path
      assert{ File.stat(c.path).mode.to_s(8).end_with?('700') }

      c.purge
    end
  end

  sub_test_case 'configured with system root directory and plugin @id' do
    setup do
      @root_dir = File.expand_path('../../tmp/buffer_file_single_root', __FILE__)
      FileUtils.rm_rf(@root_dir)

      @d = FluentPluginFileSingleBufferTest::DummyOutputPlugin.new
      @p = nil
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
      Fluent::SystemConfig.overwrite_system_config('root_dir' => @root_dir) do
        @d.configure(config_element('ROOT', '', {'@id' => 'dummy_output_with_buf'}, [config_element('buffer', '', {})]))
        @p = @d.buffer
      end

      expected_buffer_path = File.join(@root_dir, 'worker0', 'dummy_output_with_buf', 'buffer', "fsb.*.buf")
      expected_buffer_dir = File.dirname(expected_buffer_path)
      assert_equal expected_buffer_path, @d.buffer.path
      assert_false Dir.exist?(expected_buffer_dir)

      @p.start
      assert Dir.exist?(expected_buffer_dir)
    end
  end

  sub_test_case 'buffer plugin configuration errors' do
    data('tag and key' => 'tag,key',
         'multiple keys' => 'key1,key2')
    test 'invalid chunk keys' do |param|
      assert_raise Fluent::ConfigError do
        @d = create_driver(%[
          <buffer #{param}>
            @type file_single
            path #{PATH}
            calc_num_records false
          </buffer>
        ])
      end
    end

    test 'path is not specified' do
      assert_raise Fluent::ConfigError do
        @d = create_driver(%[
          <buffer tag>
            @type file_single
          </buffer>
        ])
      end
    end
  end

  sub_test_case 'there are no existing file chunks' do
    setup do
      FileUtils.rm_rf(@bufdir) if File.exist?(@bufdir)

      @d = create_driver
      @p = @d.instance.buffer
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
      @c1id = Fluent::UniqueId.generate
      p1 = File.join(@bufdir, "fsb.testing.q#{Fluent::UniqueId.hex(@c1id)}.buf")
      File.open(p1, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:58:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:58:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 13:58:22 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      t = Time.now - 50000
      File.utime(t, t, p1)

      @c2id = Fluent::UniqueId.generate
      p2 = File.join(@bufdir, "fsb.testing.q#{Fluent::UniqueId.hex(@c2id)}.buf")
      File.open(p2, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:59:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:59:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:59:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      t = Time.now - 40000
      File.utime(t, t, p2)

      @c3id = Fluent::UniqueId.generate
      p3 = File.join(@bufdir, "fsb.testing.b#{Fluent::UniqueId.hex(@c3id)}.buf")
      File.open(p3, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 14:00:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 14:00:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 14:00:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 14:00:28 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end

      @c4id = Fluent::UniqueId.generate
      p4 = File.join(@bufdir, "fsb.foo.b#{Fluent::UniqueId.hex(@c4id)}.buf")
      File.open(p4, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 14:01:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 14:01:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 14:01:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
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
      if @bufdir
        Dir.glob(File.join(@bufdir, '*')).each do |path|
          next if ['.', '..'].include?(File.basename(path))
          File.delete(path)
        end
      end
    end

    test '#resume returns staged/queued chunks with metadata' do
      @d = create_driver
      @p = @d.instance.buffer
      @p.start

      assert_equal 2, @p.stage.size
      assert_equal 2, @p.queue.size

      stage = @p.stage

      m3 = metadata()
      assert_equal @c3id, stage[m3].unique_id
      assert_equal 4, stage[m3].size
      assert_equal :staged, stage[m3].state

      m4 = metadata(tag: 'foo')
      assert_equal @c4id, stage[m4].unique_id
      assert_equal 3, stage[m4].size
      assert_equal :staged, stage[m4].state
    end

    test '#resume returns queued chunks ordered by last modified time (FIFO)' do
      @d = create_driver
      @p = @d.instance.buffer
      @p.start

      assert_equal 2, @p.stage.size
      assert_equal 2, @p.queue.size

      queue = @p.queue

      assert{ queue[0].modified_at <= queue[1].modified_at }

      assert_equal @c1id, queue[0].unique_id
      assert_equal :queued, queue[0].state
      assert_equal 'testing', queue[0].metadata.tag
      assert_nil queue[0].metadata.variables
      assert_equal 4, queue[0].size

      assert_equal @c2id, queue[1].unique_id
      assert_equal :queued, queue[1].state
      assert_equal 'testing', queue[1].metadata.tag
      assert_nil queue[1].metadata.variables
      assert_equal 3, queue[1].size
    end

    test '#resume returns staged/queued chunks but skips size calculation by calc_num_records' do
      @d = create_driver(%[
        <buffer tag>
         @type file_single
         path #{PATH}
         calc_num_records false
        </buffer>
      ])
      @p = @d.instance.buffer
      @p.start

      assert_equal 2, @p.stage.size
      assert_equal 2, @p.queue.size

      stage = @p.stage

      m3 = metadata()
      assert_equal @c3id, stage[m3].unique_id
      assert_equal 0, stage[m3].size
      assert_equal :staged, stage[m3].state

      m4 = metadata(tag: 'foo')
      assert_equal @c4id, stage[m4].unique_id
      assert_equal 0, stage[m4].size
      assert_equal :staged, stage[m4].state
    end
  end

  sub_test_case 'there are some existing file chunks with placeholders path' do
    setup do
      @buf_ph_dir = File.expand_path('../../tmp/buffer_${test}_file_single_dir', __FILE__)
      FileUtils.rm_rf(@buf_ph_dir)
      FileUtils.mkdir_p(@buf_ph_dir)

      @c1id = Fluent::UniqueId.generate
      p1 = File.join(@buf_ph_dir, "fsb.testing.q#{Fluent::UniqueId.hex(@c1id)}.buf")
      File.open(p1, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end
      t = Time.now - 50000
      File.utime(t, t, p1)

      @c2id = Fluent::UniqueId.generate
      p2 = File.join(@buf_ph_dir, "fsb.testing.b#{Fluent::UniqueId.hex(@c2id)}.buf")
      File.open(p2, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 14:00:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
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
      FileUtils.rm_rf(@buf_ph_dir)
    end

    test '#resume returns staged/queued chunks with metadata' do
      @d = create_driver(%[
        <buffer tag>
          @type file_single
          path #{@buf_ph_dir}
        </buffer>
      ])
      @p = @d.instance.buffer
      @p.start

      assert_equal 1, @p.stage.size
      assert_equal 1, @p.queue.size
    end
  end

  sub_test_case 'there are some existing msgpack file chunks' do
    setup do
      packer = Fluent::MessagePackFactory.packer
      @c1id = Fluent::UniqueId.generate
      p1 = File.join(@bufdir, "fsb.testing.q#{Fluent::UniqueId.hex(@c1id)}.buf")
      File.open(p1, 'wb') do |f|
        packer.write(["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}])
        packer.write(["t2.test", event_time('2016-04-17 13:58:17 -0700').to_i, {"message" => "yay"}])
        packer.write(["t3.test", event_time('2016-04-17 13:58:21 -0700').to_i, {"message" => "yay"}])
        packer.write(["t4.test", event_time('2016-04-17 13:58:22 -0700').to_i, {"message" => "yay"}])
        f.write packer.full_pack
      end
      t = Time.now - 50000
      File.utime(t, t, p1)

      @c2id = Fluent::UniqueId.generate
      p2 = File.join(@bufdir, "fsb.testing.q#{Fluent::UniqueId.hex(@c2id)}.buf")
      File.open(p2, 'wb') do |f|
        packer.write(["t1.test", event_time('2016-04-17 13:59:15 -0700').to_i, {"message" => "yay"}])
        packer.write(["t2.test", event_time('2016-04-17 13:59:17 -0700').to_i, {"message" => "yay"}])
        packer.write(["t3.test", event_time('2016-04-17 13:59:21 -0700').to_i, {"message" => "yay"}])
        f.write packer.full_pack
      end
      t = Time.now - 40000
      File.utime(t, t, p2)

      @c3id = Fluent::UniqueId.generate
      p3 = File.join(@bufdir, "fsb.testing.b#{Fluent::UniqueId.hex(@c3id)}.buf")
      File.open(p3, 'wb') do |f|
        packer.write(["t1.test", event_time('2016-04-17 14:00:15 -0700').to_i, {"message" => "yay"}])
        packer.write(["t2.test", event_time('2016-04-17 14:00:17 -0700').to_i, {"message" => "yay"}])
        packer.write(["t3.test", event_time('2016-04-17 14:00:21 -0700').to_i, {"message" => "yay"}])
        packer.write(["t4.test", event_time('2016-04-17 14:00:28 -0700').to_i, {"message" => "yay"}])
        f.write packer.full_pack
      end

      @c4id = Fluent::UniqueId.generate
      p4 = File.join(@bufdir, "fsb.foo.b#{Fluent::UniqueId.hex(@c4id)}.buf")
      File.open(p4, 'wb') do |f|
        packer.write(["t1.test", event_time('2016-04-17 14:01:15 -0700').to_i, {"message" => "yay"}])
        packer.write(["t2.test", event_time('2016-04-17 14:01:17 -0700').to_i, {"message" => "yay"}])
        packer.write(["t3.test", event_time('2016-04-17 14:01:21 -0700').to_i, {"message" => "yay"}])
        f.write packer.full_pack
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
      if @bufdir
        Dir.glob(File.join(@bufdir, '*')).each do |path|
          next if ['.', '..'].include?(File.basename(path))
          File.delete(path)
        end
      end
    end

    test '#resume returns staged/queued chunks with msgpack format' do
      @d = create_driver(%[
        <buffer tag>
         @type file_single
         path #{PATH}
         chunk_format msgpack
        </buffer>
      ])
      @p = @d.instance.buffer
      @p.start

      assert_equal 2, @p.stage.size
      assert_equal 2, @p.queue.size

      stage = @p.stage

      m3 = metadata()
      assert_equal @c3id, stage[m3].unique_id
      assert_equal 4, stage[m3].size
      assert_equal :staged, stage[m3].state

      m4 = metadata(tag: 'foo')
      assert_equal @c4id, stage[m4].unique_id
      assert_equal 3, stage[m4].size
      assert_equal :staged, stage[m4].state
    end
  end

  sub_test_case 'there are some existing file chunks, both in specified path and per-worker directory under specified path, configured as multi workers' do
    setup do
      @worker0_dir = File.join(@bufdir, "worker0")
      @worker1_dir = File.join(@bufdir, "worker1")
      FileUtils.rm_rf(@bufdir)
      FileUtils.mkdir_p(@worker0_dir)
      FileUtils.mkdir_p(@worker1_dir)

      @bufdir_chunk_1 = Fluent::UniqueId.generate
      bc1 = File.join(@bufdir, "fsb.testing.q#{Fluent::UniqueId.hex(@bufdir_chunk_1)}.buf")
      File.open(bc1, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:58:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:58:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 13:58:22 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end

      @bufdir_chunk_2 = Fluent::UniqueId.generate
      bc2 = File.join(@bufdir, "fsb.testing.q#{Fluent::UniqueId.hex(@bufdir_chunk_2)}.buf")
      File.open(bc2, 'wb') do |f|
        f.write ["t1.test", event_time('2016-04-17 13:58:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t2.test", event_time('2016-04-17 13:58:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t3.test", event_time('2016-04-17 13:58:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        f.write ["t4.test", event_time('2016-04-17 13:58:22 -0700').to_i, {"message" => "yay"}].to_json + "\n"
      end

      @worker_dir_chunk_1 = Fluent::UniqueId.generate
      wc0_1 = File.join(@worker0_dir, "fsb.testing.q#{Fluent::UniqueId.hex(@worker_dir_chunk_1)}.buf")
      wc1_1 = File.join(@worker1_dir, "fsb.testing.q#{Fluent::UniqueId.hex(@worker_dir_chunk_1)}.buf")
      [wc0_1, wc1_1].each do |chunk_path|
        File.open(chunk_path, 'wb') do |f|
          f.write ["t1.test", event_time('2016-04-17 13:59:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t2.test", event_time('2016-04-17 13:59:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t3.test", event_time('2016-04-17 13:59:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        end
      end

      @worker_dir_chunk_2 = Fluent::UniqueId.generate
      wc0_2 = File.join(@worker0_dir, "fsb.testing.b#{Fluent::UniqueId.hex(@worker_dir_chunk_2)}.buf")
      wc1_2 = File.join(@worker1_dir, "fsb.foo.b#{Fluent::UniqueId.hex(@worker_dir_chunk_2)}.buf")
      [wc0_2, wc1_2].each do |chunk_path|
        File.open(chunk_path, 'wb') do |f|
          f.write ["t1.test", event_time('2016-04-17 14:00:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t2.test", event_time('2016-04-17 14:00:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t3.test", event_time('2016-04-17 14:00:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t4.test", event_time('2016-04-17 14:00:28 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        end
      end

      @worker_dir_chunk_3 = Fluent::UniqueId.generate
      wc0_3 = File.join(@worker0_dir, "fsb.bar.b#{Fluent::UniqueId.hex(@worker_dir_chunk_3)}.buf")
      wc1_3 = File.join(@worker1_dir, "fsb.baz.b#{Fluent::UniqueId.hex(@worker_dir_chunk_3)}.buf")
      [wc0_3, wc1_3].each do |chunk_path|
        File.open(chunk_path, 'wb') do |f|
          f.write ["t1.test", event_time('2016-04-17 14:01:15 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t2.test", event_time('2016-04-17 14:01:17 -0700').to_i, {"message" => "yay"}].to_json + "\n"
          f.write ["t3.test", event_time('2016-04-17 14:01:21 -0700').to_i, {"message" => "yay"}].to_json + "\n"
        end
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

    test 'worker(id=0) #resume returns staged/queued chunks with metadata, not only in worker dir, including the directory specified by path' do
      ENV['SERVERENGINE_WORKER_ID'] = '0'

      buf_conf = config_element('buffer', '', {'path' => @bufdir})
      @d = FluentPluginFileSingleBufferTest::DummyOutputPlugin.new
      with_worker_config(workers: 2, worker_id: 0) do
        @d.configure(config_element('output', '', {}, [buf_conf]))
      end

      @d.start
      @p = @d.buffer

      assert_equal 2, @p.stage.size
      assert_equal 3, @p.queue.size

      stage = @p.stage

      m1 = metadata(tag: 'testing')
      assert_equal @worker_dir_chunk_2, stage[m1].unique_id
      assert_equal 4, stage[m1].size
      assert_equal :staged, stage[m1].state

      m2 = metadata(tag: 'bar')
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
      @d = FluentPluginFileSingleBufferTest::DummyOutputPlugin.new
      with_worker_config(workers: 2, worker_id: 1) do
        @d.configure(config_element('output', '', {}, [buf_conf]))
      end

      @d.start
      @p = @d.buffer

      assert_equal 2, @p.stage.size
      assert_equal 1, @p.queue.size

      stage = @p.stage

      m1 = metadata(tag: 'foo')
      assert_equal @worker_dir_chunk_2, stage[m1].unique_id
      assert_equal 4, stage[m1].size
      assert_equal :staged, stage[m1].state

      m2 = metadata(tag: 'baz')
      assert_equal @worker_dir_chunk_3, stage[m2].unique_id
      assert_equal 3, stage[m2].size
      assert_equal :staged, stage[m2].state

      queue = @p.queue

      assert_equal @worker_dir_chunk_1, queue[0].unique_id
      assert_equal 3, queue[0].size
      assert_equal :queued, queue[0].state
    end
  end

  sub_test_case 'there are existing broken file chunks' do
    setup do
      FileUtils.rm_rf(@bufdir) rescue nil
      FileUtils.mkdir_p(@bufdir)
    end

    teardown do
      return unless @p

      @p.stop unless @p.stopped?
      @p.before_shutdown unless @p.before_shutdown?
      @p.shutdown unless @p.shutdown?
      @p.after_shutdown unless @p.after_shutdown?
      @p.close unless @p.closed?
      @p.terminate unless @p.terminated?
    end

    test '#resume backups empty chunk' do
      id_output = 'backup_test'
      @d = create_driver(%[
        @id #{id_output}
        <buffer tag>
         @type file_single
         path #{PATH}
        </buffer>
      ])
      @p = @d.instance.buffer

      c1id = Fluent::UniqueId.generate
      p1 = File.join(@bufdir, "fsb.foo.b#{Fluent::UniqueId.hex(c1id)}.buf")
      File.open(p1, 'wb') { |f| } # create empty chunk file

      Fluent::SystemConfig.overwrite_system_config('root_dir' => @bufdir) do
        @p.start
      end

      assert { not File.exist?(p1) }
      assert { File.exist?("#{@bufdir}/backup/worker0/#{id_output}/#{@d.instance.dump_unique_id_hex(c1id)}.log") }
    end

    test '#resume throws away broken chunk with disable_chunk_backup' do
      id_output = 'backup_test'
      @d = create_driver(%[
        @id #{id_output}
        <buffer tag>
         @type file_single
         path #{PATH}
         disable_chunk_backup true
        </buffer>
      ])
      @p = @d.instance.buffer

      c1id = Fluent::UniqueId.generate
      p1 = File.join(@bufdir, "fsb.foo.b#{Fluent::UniqueId.hex(c1id)}.buf")
      File.open(p1, 'wb') { |f| } # create empty chunk file

      Fluent::SystemConfig.overwrite_system_config('root_dir' => @bufdir) do
        @p.start
      end

      assert { not File.exist?(p1) }
      assert { not File.exist?("#{@bufdir}/backup/worker0/#{id_output}/#{@d.instance.dump_unique_id_hex(c1id)}.log") }
    end
  end

  sub_test_case 'evacuate_chunk' do
    def setup
      Fluent::Test.setup

      @now = Time.local(2025, 5, 30, 17, 0, 0)
      @base_dir = File.expand_path("../../tmp/evacuate_chunk", __FILE__)
      @buf_dir = File.join(@base_dir, "buffer")
      @root_dir = File.join(@base_dir, "root")
      FileUtils.mkdir_p(@root_dir)

      Fluent::SystemConfig.overwrite_system_config("root_dir" => @root_dir) do
        Timecop.freeze(@now)
        yield
      end
    ensure
      Timecop.return
      FileUtils.rm_rf(@base_dir)
    end

    def start_plugin(plugin)
      plugin.start
      plugin.after_start
    end

    def stop_plugin(plugin)
      plugin.stop unless plugin.stopped?
      plugin.before_shutdown unless plugin.before_shutdown?
      plugin.shutdown unless plugin.shutdown?
      plugin.after_shutdown unless plugin.after_shutdown?
      plugin.close unless plugin.closed?
      plugin.terminate unless plugin.terminated?
    end

    def configure_output(id, chunk_key, buffer_conf)
      output = FluentPluginFileSingleBufferTest::DummyErrorOutputPlugin.new
      output.configure(
        config_element('ROOT', '', {'@id' => id}, [config_element('buffer', chunk_key, buffer_conf)])
      )
      yield output
    ensure
      stop_plugin(output)
    end

    def wait(sec: 4)
      waiting(sec) do
        Thread.pass until yield
      end
    end

    def emit_events(output, tag, es)
      output.interrupt_flushes
      output.emit_events("test.1", dummy_event_stream)
      @now += 1
      Timecop.freeze(@now)
      output.enqueue_thread_wait
      output.flush_thread_wakeup
    end

    def proceed_to_next_retry(output)
      @now += 1
      Timecop.freeze(@now)
      output.flush_thread_wakeup
    end

    def dummy_event_stream
      Fluent::ArrayEventStream.new([
        [ event_time("2025-05-30 10:00:00"), {"message" => "data1"} ],
        [ event_time("2025-05-30 10:10:00"), {"message" => "data2"} ],
        [ event_time("2025-05-30 10:20:00"), {"message" => "data3"} ],
      ])
    end

    def evacuate_dir(plugin_id)
      File.join(@root_dir, "buffer", plugin_id)
    end

    test 'can recover by putting back evacuated chunk files' do
      plugin_id = "test_output"
      tag = "test.1"
      buffer_conf = {
        "path" => @buf_dir,
        "flush_mode" => "interval",
        "flush_interval" => "1s",
        "retry_type" => "periodic",
        "retry_max_times" => 1,
        "retry_randomize" => false,
      }

      # Fail flushing and reach retry limit
      configure_output(plugin_id, "tag", buffer_conf) do |output|
        start_plugin(output)

        emit_events(output, tag, dummy_event_stream)
        wait { output.write_count == 1 and output.num_errors == 1 }

        proceed_to_next_retry(output)
        wait { output.write_count == 2 and output.num_errors == 2 }
        wait { Dir.empty?(@buf_dir) }

        # Assert evacuated files
        evacuated_files = Dir.children(evacuate_dir(plugin_id)).map do |child_name|
          File.join(evacuate_dir(plugin_id), child_name)
        end
        assert { evacuated_files.size == 1 } # .log

        # Put back evacuated chunk files for recovery
        FileUtils.move(evacuated_files, @buf_dir)
      end

      # Restart plugin to load the chunk files that were put back
      written_data = []
      configure_output(plugin_id, "tag", buffer_conf) do |output|
        output.recover
        output.register_write do |chunk|
          written_data << chunk.read
        end
        start_plugin(output)

        wait { not written_data.empty? }
      end

      # Assert the recovery success
      assert { written_data.length == 1 }

      expected_records = []
      dummy_event_stream.each do |(time, record)|
        expected_records << [tag, time.to_i, record]
      end

      actual_records = StringIO.open(written_data.first) do |io|
        io.each_line.map do |line|
          JSON.parse(line)
        end
      end

      assert_equal(expected_records, actual_records)
    end
  end
end
