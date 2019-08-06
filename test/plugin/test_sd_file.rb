require_relative '../helper'
require 'fluent/plugin/sd_file'
require 'fileutils'
require 'json'

class SdFileTest < ::Test::Unit::TestCase
  setup do
    @dir = File.expand_path('data/sd_file', __dir__)
    FileUtils.mkdir_p(File.join(@dir, 'tmp'))
  end

  teardown do
    FileUtils.rm_r(File.join(@dir, 'tmp'))
  end

  sub_test_case 'configure' do
    test 'load yml' do
      sdf = Fluent::Plugin::SdFile.new
      sdf.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'config.yml') }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:file, '127.0.0.1', 24224, 'test1', 1, false, 'user1', 'pass1', 'key1'), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:file, '127.0.0.1', 24225, nil, 1), sdf.services[1]
    end

    test 'load yaml' do
      sdf = Fluent::Plugin::SdFile.new
      sdf.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'config.yaml') }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:file, '127.0.0.1', 24224, 'test1', 1, false, 'user1', 'pass1', 'key1'), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:file, '127.0.0.1', 24225, nil, 1), sdf.services[1]
    end

    test 'load json' do
      sdf = Fluent::Plugin::SdFile.new
      sdf.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'config.json') }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:file, '127.0.0.1', 24224, 'test1', 1, false, 'user1', 'pass1', 'key1'), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:file, '127.0.0.1', 24225, nil, 1), sdf.services[1]
    end

    test 'regard as yaml if ext is not givened' do
      sdf = Fluent::Plugin::SdFile.new
      sdf.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'config') }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:file, '127.0.0.1', 24224, 'test1', 1, false, 'user1', 'pass1', 'key1'), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:file, '127.0.0.1', 24225, nil, 1), sdf.services[1]
    end

    test 'raise an error if config has error' do
      sdf = Fluent::Plugin::SdFile.new
      e = assert_raise Fluent::ConfigError do
        sdf.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'invalid_config.yaml') }))
      end
      assert_match(/path=/, e.message)
    end

    test 'raise an error if config file does not exist' do
      sdf = Fluent::Plugin::SdFile.new
      e = assert_raise Fluent::ConfigError do
        sdf.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'invalid_not_found.json') }))
      end
      assert_match(/not found/, e.message)
    end
  end

  sub_test_case '#start' do
    module TestStatEventHelperWrapper
      # easy to control statsevent
      def event_loop_attach(watcher)
        unless watcher.is_a?(Fluent::Plugin::SdFile::StatWatcher)
          super
          return
        end

        @test_stat_event_helper_wrapper_watchers ||= []
        @test_stat_event_helper_wrapper_watchers << watcher

        @test_stat_event_helper_wrapper_context = Fiber.new do
          loop do
            @test_stat_event_helper_wrapper_watchers.each do |w|
              w.on_change('old', 'new')
            end

            if Fiber.yield == :finish
              break
            end
          end
        end
        resume
      end

      def resume
        @test_stat_event_helper_wrapper_context.resume(:resume)
      end

      def shutdown
        super

        if @test_stat_event_helper_wrapper_context
          @test_stat_event_helper_wrapper_context.resume(:finish)
        end
      end
    end

    def create_tmp_config(path, body)
      File.write(File.join(@dir, 'tmp', path), body)
    end

    setup do
      sdf = Fluent::Plugin::SdFile.new
      @sd_file = sdf
    end

    teardown do
      if @sd_file
        @sd_file.stop unless @sd_file.stopped?
        @sd_file.before_shutdown unless @sd_file.before_shutdown?
        @sd_file.shutdown unless @sd_file.shutdown?
        @sd_file.after_shutdown unless @sd_file.after_shutdown?
        @sd_file.close unless @sd_file.closed?
        @sd_file.terminate unless @sd_file.terminated?
      end
    end

    test 'Skip if file is not updated' do
      @sd_file.extend(TestStatEventHelperWrapper)

      create_tmp_config('config.json', JSON.generate([{ port: 1233, host: '127.0.0.1' }]))
      @sd_file.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'config.yml') }))
      queue = []
      mock.proxy(@sd_file).refresh_file(queue).twice

      @sd_file.start(queue)
      assert_empty queue

      @sd_file.resume
      assert_empty queue
    end

    test 'Skip if file is invalid contents' do
      @sd_file.extend(TestStatEventHelperWrapper)

      create_tmp_config('config.json', JSON.generate([{ port: 1233, host: '127.0.0.1' }]))
      @sd_file.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'config.yml') }))

      queue = []
      @sd_file.start(queue)

      mock.proxy(@sd_file).refresh_file(queue).once
      create_tmp_config('test.json', 'invalid contents')
      @sd_file.resume

      assert_empty queue
    end

    test 'if service is updated, service_in and service_out event happen' do
      @sd_file.extend(TestStatEventHelperWrapper)

      create_tmp_config('test.json', JSON.generate([{ port: 1233, host: '127.0.0.1' }]))
      @sd_file.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'tmp/test.json') }))

      queue = []
      @sd_file.start(queue)
      create_tmp_config('test.json', JSON.generate([{ port: 1234, host: '127.0.0.1' }]))
      @sd_file.resume

      assert_equal 2, queue.size
      join = queue.shift
      drain = queue.shift
      assert_equal Fluent::Plugin::ServiceDiscovery::SERVICE_IN, join.type
      assert_equal 1234, join.service.port
      assert_equal '127.0.0.1', join.service.host

      assert_equal Fluent::Plugin::ServiceDiscovery::SERVICE_OUT, drain.type
      assert_equal 1233, drain.service.port
      assert_equal '127.0.0.1', drain.service.host
    end

    test 'if service is deleted, service_out event happens' do
      @sd_file.extend(TestStatEventHelperWrapper)

      create_tmp_config('test.json', JSON.generate([{ port: 1233, host: '127.0.0.1' }, { port: 1234, host: '127.0.0.2' }]))
      @sd_file.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'tmp/test.json') }))

      queue = []
      @sd_file.start(queue)
      create_tmp_config('test.json', JSON.generate([{ port: 1233, host: '127.0.0.1' }]))
      @sd_file.resume

      assert_equal 1, queue.size
      drain = queue.shift
      assert_equal Fluent::Plugin::ServiceDiscovery::SERVICE_OUT, drain.type
      assert_equal 1234, drain.service.port
      assert_equal '127.0.0.2', drain.service.host
    end

    test 'if new service is added, service_in event happens' do
      @sd_file.extend(TestStatEventHelperWrapper)

      create_tmp_config('test.json', JSON.generate([{ port: 1233, host: '127.0.0.1' }]))
      @sd_file.configure(config_element('service_discovery', '', { 'path' => File.join(@dir, 'tmp/test.json') }))

      queue = []
      @sd_file.start(queue)
      create_tmp_config('test.json', JSON.generate([{ port: 1233, host: '127.0.0.1' }, { port: 1234, host: '127.0.0.2' }]))
      @sd_file.resume

      assert_equal 1, queue.size
      join = queue.shift
      assert_equal Fluent::Plugin::ServiceDiscovery::SERVICE_IN, join.type
      assert_equal 1234, join.service.port
      assert_equal '127.0.0.2', join.service.host
    end
  end
end
