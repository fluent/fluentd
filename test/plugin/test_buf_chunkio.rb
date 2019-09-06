require_relative '../helper'
require 'fluent/plugin/buf_chunkio'
require 'fluent/plugin/output'

module FluentPluginChunkioBufferTest
  class DummyOutputPlugin < Fluent::Plugin::Output
    Fluent::Plugin.register_output('buffer_chunkio_output', self)

    config_section :buffer do
      config_set_default :@type, 'chunkio'
    end

    def multi_workers_ready?;
      true
    end

    def write(chunk)
      # drop
    end
  end
end

class ChunkioBufferTest < Test::Unit::TestCase
  BUF_PATH = File.expand_path('../../tmp/buffer_chunkio', __dir__)

  setup do
    Fluent::Test.setup

    @chunkdir = BUF_PATH
    @stream_name = 'buffer'
    @d = FluentPluginChunkioBufferTest::DummyOutputPlugin.new
    FileUtils.mkdir_p(@chunkdir)
  end

  teardown do
    FileUtils.rm_r(@chunkdir) rescue nil
  end

  def buffer_config(config = {})
    elems = { 'path' => @chunkdir, 'stream_name' => @stream_name }
    config_element('buffer', '', elems.merge(config))
  end

  sub_test_case '#configure' do
    setup do
      @b = Fluent::Plugin::ChunkioBuffer.new
      @b.owner = @d
    end

    test 'path has context, stream_name, file_name' do
      path = File.expand_path('../../tmp/buffer_chunkio2', __dir__)
      stream_name = 'stream_name2'
      @b.configure('path' => path, 'stream_name' => stream_name)
      assert_equal File.join(path, stream_name, 'cio.*.buf'), @b.path
    end

    test 'path can be set filename suffix' do
      @b.configure(buffer_config('file_suffix' => 'log'))
      assert_equal File.join(@chunkdir, @stream_name, 'cio.*.log'), @b.path
    end

    data(
      'path is defined at system' => [
        {},
        File.join(BUF_PATH, 'worker0', 'dummy_output_with_chunkio_buf')
      ],
      'path is defined at both system and buffer' => [
        { 'path' => File.expand_path('../../tmp/buffer_chunkio2', __dir__) },
        File.expand_path('../../tmp/buffer_chunkio2', __dir__)
      ],
    )
    test 'can see the root_dir' do |args|
      opt, expect = args
      Fluent::SystemConfig.overwrite_system_config('root_dir' => @chunkdir) do
        @d.configure(config_element('ROOT', '', '@id' => 'dummy_output_with_chunkio_buf'))
        @b.configure(config_element('buffer', '', { 'stream_name' => @stream_name }.merge(opt)))
      end

      assert_equal File.join(expect, @stream_name, 'cio.*.buf'), @b.path
    end

    test 'if multiple worker mode, add worker direcotry' do
      Fluent::SystemConfig.overwrite_system_config('workers' => 2) do
        @b.configure(buffer_config)
      end

      assert_equal File.join(@chunkdir, @stream_name, 'worker0', 'cio.*.buf'), @b.path
    end

    test 'path includes workerid and id if multiple worker mode and path is defined in system' do
      Fluent::SystemConfig.overwrite_system_config('workers' => 2, 'root_dir' => @chunkdir) do
        @d.configure(config_element('ROOT', '', '@id' => 'dummy_output_with_chunkio_buf'))
        @b.configure(config_element('buffer', '', 'stream_name' => @stream_name))
      end

      assert_equal File.join(@chunkdir, 'worker0', 'dummy_output_with_chunkio_buf', @stream_name, 'cio.*.buf'), @b.path
    end

    data(
      'path is empty' => { 'path' => '', 'stream_name' => 'log' },
      'path is nil' => { 'path' => nil, 'stream_name' => 'log' },
      'stream_name is empty' => { 'path' => BUF_PATH, 'stream_name' => '' },
      'stream_name is nil' => { 'path' => BUF_PATH, 'stream_name' => nil },
      'path includes *' => { 'path' => BUF_PATH + '/cio.*.buf', 'stream_name' => 'log' },
    )
    test 'raise an error when' do |opt|
      assert_raise Fluent::ConfigError do
        @b.configure(opt)
      end
    end
  end

  sub_test_case '#start' do
    setup do
      @path = File.expand_path('../../tmp/buffer_chunkio_start', __dir__)
       FileUtils.rmn_r(@path) rescue nil
      @b = Fluent::Plugin::ChunkioBuffer.new
      @b.owner = @d
    end

    teardown do
      if @d
        @d.stop unless @d.stopped?
        @d.before_shutdown unless @d.before_shutdown?
        @d.shutdown unless @d.shutdown?
        @d.after_shutdown unless @d.after_shutdown?
        @d.close unless @d.closed?
        @d.terminate unless @d.terminated?
      end

      FileUtils.rm_r(@path) rescue nil
    end

    test 'create directory' do
      assert_false File.exist?(@path)
      @b.configure(buffer_config('path' => @path))
      @b.start
      assert File.exist?(@path)
    end

    test 'create directory with specify permission' do
      assert_false File.exist?(@path)
      @b.configure(buffer_config('path' => @path, 'dir_permission' => '700'))
      @b.start
      assert File.stat(File.join(@path, @stream_name)).mode.to_s(8).end_with?('700')
    end
  end
end
