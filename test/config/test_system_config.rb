require_relative '../helper'
require 'fluent/configurable'
require 'fluent/config/element'
require 'fluent/config/section'
require 'fluent/system_config'

module Fluent::Config
  class FakeSupervisor
    attr_writer :log_level

    def initialize(**opt)
      @system_config = nil
      @cl_opt = {
        workers: nil,
        restart_worker_interval: nil,
        root_dir: nil,
        log_level: Fluent::Log::LEVEL_INFO,
        suppress_interval: nil,
        suppress_config_dump: nil,
        suppress_repeated_stacktrace: nil,
        log_event_label: nil,
        log_event_verbose: nil,
        without_source: nil,
        with_source_only: nil,
        enable_input_metrics: nil,
        enable_size_metrics: nil,
        emit_error_log_interval: nil,
        file_permission: nil,
        dir_permission: nil,
      }.merge(opt)
    end

    def for_system_config
      opt = {}
      # this is copy from Supervisor#build_system_config
      Fluent::SystemConfig::SYSTEM_CONFIG_PARAMETERS.each do |param|
        if @cl_opt.key?(param) && !@cl_opt[param].nil?
          if param == :log_level && @cl_opt[:log_level] == Fluent::Log::LEVEL_INFO
            # info level can't be specified via command line option.
            # log_level is info here, it is default value and <system>'s log_level should be applied if exists.
            next
          end

          opt[param] = @cl_opt[param]
        end
      end

      opt
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
      sc.overwrite_variables(**s.for_system_config)
      assert_equal(1, sc.workers)
      assert_equal(0, sc.restart_worker_interval)
      assert_nil(sc.root_dir)
      assert_equal(Fluent::Log::LEVEL_INFO, sc.log_level)
      assert_nil(sc.suppress_repeated_stacktrace)
      assert_nil(sc.ignore_repeated_log_interval)
      assert_nil(sc.emit_error_log_interval)
      assert_nil(sc.suppress_config_dump)
      assert_nil(sc.without_source)
      assert_nil(sc.with_source_only)
      assert_true(sc.enable_input_metrics)
      assert_nil(sc.enable_size_metrics)
      assert_nil(sc.enable_msgpack_time_support)
      assert(!sc.enable_jit)
      assert_nil(sc.log.path)
      assert_equal(:text, sc.log.format)
      assert_equal('%Y-%m-%d %H:%M:%S %z', sc.log.time_format)
      assert_equal(:none, sc.log.forced_stacktrace_level)
    end

    data(
      'workers' => ['workers', 3],
      'restart_worker_interval' => ['restart_worker_interval', 60],
      'root_dir' => ['root_dir', File.join(TMP_DIR, 'root')],
      'log_level' => ['log_level', 'error'],
      'suppress_repeated_stacktrace' => ['suppress_repeated_stacktrace', true],
      'ignore_repeated_log_interval' => ['ignore_repeated_log_interval', 10],
      'log_event_verbose' => ['log_event_verbose', true],
      'suppress_config_dump' => ['suppress_config_dump', true],
      'without_source' => ['without_source', true],
      'with_source_only' => ['with_source_only', true],
      'strict_config_value' => ['strict_config_value', true],
      'enable_msgpack_time_support' => ['enable_msgpack_time_support', true],
      'enable_input_metrics' => ['enable_input_metrics', false],
      'enable_size_metrics' => ['enable_size_metrics', true],
      'enable_jit' => ['enable_jit', true],
    )
    test "accepts parameters" do |(k, v)|
      conf = parse_text(<<-EOS)
          <system>
            #{k} #{v}
          </system>
      EOS
      s = FakeSupervisor.new
      sc = Fluent::SystemConfig.new(conf)
      sc.overwrite_variables(**s.for_system_config)
      if k == 'log_level'
        assert_equal(Fluent::Log::LEVEL_ERROR, sc.__send__(k))
      else
        assert_equal(v, sc.__send__(k))
      end
    end

    test "log parameters" do
      conf = parse_text(<<-EOS)
          <system>
            <log>
              path /tmp/fluentd.log
              format json
              time_format %Y
              forced_stacktrace_level info
            </log>
          </system>
      EOS
      s = FakeSupervisor.new
      sc = Fluent::SystemConfig.new(conf)
      sc.overwrite_variables(**s.for_system_config)
      assert_equal('/tmp/fluentd.log', sc.log.path)
      assert_equal(:json, sc.log.format)
      assert_equal('%Y', sc.log.time_format)
      assert_equal(Fluent::Log::LEVEL_INFO, sc.log.forced_stacktrace_level)
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
      s = FakeSupervisor.new(log_level: level)
      sc = Fluent::SystemConfig.new(conf)
      sc.overwrite_variables(**s.for_system_config)
      assert_equal(level, sc.log_level)
    end

    sub_test_case "log rotation" do
      data('daily' => "daily",
           'weekly' => 'weekly',
           'monthly' => 'monthly')
      test "strings for rotate_age" do |age|
        conf = parse_text(<<-EOS)
          <system>
            <log>
              rotate_age #{age}
            </log>
          </system>
        EOS
        sc = Fluent::SystemConfig.new(conf)
        assert_equal(age, sc.log.rotate_age)
      end

      test "numeric number for rotate age" do
        conf = parse_text(<<-EOS)
          <system>
            <log>
              rotate_age 3
            </log>
          </system>
        EOS
        sc = Fluent::SystemConfig.new(conf)
        assert_equal(3, sc.log.rotate_age)
      end

      data(h: ['100', 100],
           k: ['1k', 1024],
           m: ['1m', 1024 * 1024],
           g: ['1g', 1024 * 1024 * 1024])
      test "numeric and SI prefix for rotate_size" do |(label, size)|
        conf = parse_text(<<-EOS)
          <system>
            <log>
              rotate_size #{label}
            </log>
          </system>
        EOS
        sc = Fluent::SystemConfig.new(conf)
        assert_equal(size, sc.log.rotate_size)
      end
    end

    test "source-only-buffer parameters" do
      conf = parse_text(<<~EOS)
        <system>
          <source_only_buffer>
            flush_thread_count 4
            overflow_action throw_exception
            path /tmp/source-only-buffer
            flush_interval 1
            chunk_limit_size 100
            total_limit_size 1000
            compress gzip
          </source_only_buffer>
        </system>
      EOS
      s = FakeSupervisor.new
      sc = Fluent::SystemConfig.new(conf)
      sc.overwrite_variables(**s.for_system_config)

      assert_equal(
        [
          4,
          :throw_exception,
          "/tmp/source-only-buffer",
          1,
          100,
          1000,
          :gzip,
        ],
        [
          sc.source_only_buffer.flush_thread_count,
          sc.source_only_buffer.overflow_action,
          sc.source_only_buffer.path,
          sc.source_only_buffer.flush_interval,
          sc.source_only_buffer.chunk_limit_size,
          sc.source_only_buffer.total_limit_size,
          sc.source_only_buffer.compress,
        ]
      )
    end
  end
end
