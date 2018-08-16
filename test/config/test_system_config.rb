require_relative '../helper'
require 'fluent/configurable'
require 'fluent/config/element'
require 'fluent/config/section'
require 'fluent/system_config'

module Fluent::Config
  class FakeLoggerInitializer
    attr_accessor :level
    def initialize
      @level = nil
    end
  end

  class FakeSupervisor
    attr_writer :log_level

    def initialize
      @workers = nil
      @root_dir = nil
      @log = FakeLoggerInitializer.new
      @log_level = Fluent::Log::LEVEL_INFO
      @suppress_interval = nil
      @suppress_config_dump = nil
      @suppress_repeated_stacktrace = nil
      @log_event_label = nil
      @log_event_verbose = nil
      @without_source = nil
      @emit_error_log_interval = nil
      @file_permission = nil
      @dir_permission = nil
    end
  end

  class TestSystemConfig < ::Test::Unit::TestCase
    TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/tmp/system_config/#{ENV['TEST_ENV_NUMBER']}")

    def parse_text(text)
      basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
      Fluent::Config.parse(text, '(test)', basepath, true).elements.find { |e| e.name == 'system' }
    end

    test 'should not override default configurations when no parameters' do
      conf = parse_text(<<-EOS)
        <system>
        </system>
      EOS
      s = FakeSupervisor.new
      sc = Fluent::SystemConfig.new(conf)
      sc.apply(s)
      assert_equal(1, sc.workers)
      assert_nil(sc.root_dir)
      assert_nil(sc.log_level)
      assert_nil(sc.suppress_repeated_stacktrace)
      assert_nil(sc.emit_error_log_interval)
      assert_nil(sc.suppress_config_dump)
      assert_nil(sc.without_source)
      assert_equal(:text, sc.log.format)
      assert_equal('%Y-%m-%d %H:%M:%S %z', sc.log.time_format)
      assert_equal(1, s.instance_variable_get(:@workers))
      assert_nil(s.instance_variable_get(:@root_dir))
      assert_equal(Fluent::Log::LEVEL_INFO, s.instance_variable_get(:@log_level))
      assert_nil(s.instance_variable_get(:@suppress_repeated_stacktrace))
      assert_nil(s.instance_variable_get(:@emit_error_log_interval))
      assert_nil(s.instance_variable_get(:@suppress_config_dump))
      assert_nil(s.instance_variable_get(:@log_event_verbose))
      assert_nil(s.instance_variable_get(:@without_source))
      assert_nil(s.instance_variable_get(:@file_permission))
      assert_nil(s.instance_variable_get(:@dir_permission))
    end

    data(
      'workers' => ['workers', 3],
      'root_dir' => ['root_dir', File.join(TMP_DIR, 'root')],
      'log_level' => ['log_level', 'error'],
      'suppress_repeated_stacktrace' => ['suppress_repeated_stacktrace', true],
      'emit_error_log_interval' => ['emit_error_log_interval', 60],
      'log_event_verbose' => ['log_event_verbose', true],
      'suppress_config_dump' => ['suppress_config_dump', true],
      'without_source' => ['without_source', true],
    )
    test "accepts parameters" do |(k, v)|
      conf = parse_text(<<-EOS)
          <system>
            #{k} #{v}
          </system>
      EOS
      s = FakeSupervisor.new
      sc = Fluent::SystemConfig.new(conf)
      sc.apply(s)
      assert_not_nil(sc.instance_variable_get("@#{k}"))
      key = (k == 'emit_error_log_interval' ? 'suppress_interval' : k)
      assert_not_nil(s.instance_variable_get("@#{key}"))
    end

    test "log parameters" do
      conf = parse_text(<<-EOS)
          <system>
            <log>
              format json
              time_format %Y
            </log>
          </system>
      EOS
      s = FakeSupervisor.new
      sc = Fluent::SystemConfig.new(conf)
      sc.apply(s)
      assert_equal(:json, sc.log.format)
      assert_equal('%Y', sc.log.time_format)
    end

    data(
      'foo' => ['foo', 'bar'],
      'hoge' => ['hoge', 'fuga'],
    )
    test "should not affect settable parameters with unknown parameters" do |(k, v)|
      s = FakeSupervisor.new
      sc = Fluent::SystemConfig.new({k => v})
      sc.apply(s)
      assert_equal(1, s.instance_variable_get(:@workers))
      assert_nil(s.instance_variable_get(:@root_dir))
      assert_equal(Fluent::Log::LEVEL_INFO, s.instance_variable_get(:@log_level))
      assert_nil(s.instance_variable_get(:@suppress_repeated_stacktrace))
      assert_nil(s.instance_variable_get(:@emit_error_log_interval))
      assert_nil(s.instance_variable_get(:@suppress_config_dump))
      assert_nil(s.instance_variable_get(:@log_event_verbose))
      assert_nil(s.instance_variable_get(:@without_source))
      assert_nil(s.instance_variable_get(:@file_permission))
      assert_nil(s.instance_variable_get(:@dir_permission))
    end

    data('trace' => Fluent::Log::LEVEL_TRACE,
         'debug' => Fluent::Log::LEVEL_DEBUG,
         'info' => Fluent::Log::LEVEL_INFO,
         'warn' => Fluent::Log::LEVEL_WARN,
         'error' => Fluent::Log::LEVEL_ERROR,
         'fatal' => Fluent::Log::LEVEL_FATAL)
    test 'log_level is applied when log_level related command line option is not passed' do |level|
      conf = parse_text(<<-EOS)
        <system>
          log_level #{Fluent::Log::LEVEL_TEXT[level]}
        </system>
      EOS
      s = FakeSupervisor.new
      sc = Fluent::SystemConfig.new(conf)
      sc.attach(s)
      sc.apply(s)
      assert_equal(level, s.instance_variable_get("@log").level)
    end

    # info is removed because info level can't be specified via command line
    data('trace' => Fluent::Log::LEVEL_TRACE,
         'debug' => Fluent::Log::LEVEL_DEBUG,
         'warn' => Fluent::Log::LEVEL_WARN,
         'error' => Fluent::Log::LEVEL_ERROR,
         'fatal' => Fluent::Log::LEVEL_FATAL)
    test 'log_level is ignored when log_level related command line option is passed' do |level|
      conf = parse_text(<<-EOS)
        <system>
          log_level info
        </system>
      EOS
      s = FakeSupervisor.new
      s.log_level = level
      sc = Fluent::SystemConfig.new(conf)
      sc.attach(s)
      sc.apply(s)
      assert_equal(level, s.instance_variable_get("@log").level)
    end

    test 'process global overridable variables' do
      conf = parse_text(<<-EOS)
        <system>
          file_permission 0655
          dir_permission 0765
        </system>
      EOS
      s = FakeSupervisor.new
      sc = Fluent::SystemConfig.new(conf)
      sc.attach(s)
      sc.apply(s)
      assert_equal(0655, s.instance_variable_get(:@file_permission))
      assert_equal(0765, s.instance_variable_get(:@dir_permission))
    end
  end
end
