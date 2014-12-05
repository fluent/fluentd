require_relative '../helper'
require 'fluent/configurable'
require 'fluent/config/element'
require 'fluent/config/section'
require 'fluent/supervisor'

module Fluent::Config
  class FakeLoggerInitializer
    attr_accessor :level
    def initalize
      @level = nil
    end
  end

  class FakeSupervisor
    def initialize
      @log = FakeLoggerInitializer.new
      @log_level = nil
      @suppress_interval = nil
      @suppress_config_dump = nil
      @suppress_repeated_stacktrace = nil
      @without_source = nil
    end
  end

  class TestSystemConfig < ::Test::Unit::TestCase

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
      sc = Fluent::Supervisor::SystemConfig.new(conf)
      sc.apply(s)
      assert_nil(sc.log_level)
      assert_nil(sc.suppress_repeated_stacktrace)
      assert_nil(sc.emit_error_log_interval)
      assert_nil(sc.suppress_config_dump)
      assert_nil(sc.without_source)
      assert_nil(s.instance_variable_get(:@log_level))
      assert_nil(s.instance_variable_get(:@suppress_repeated_stacktrace))
      assert_nil(s.instance_variable_get(:@emit_error_log_interval))
      assert_nil(s.instance_variable_get(:@suppress_config_dump))
      assert_nil(s.instance_variable_get(:@without_source))
    end

    {'log_level' => 'error',
     'suppress_repeated_stacktrace' => true,
     'emit_error_log_interval' => 60,
     'suppress_config_dump' => true,
     'without_source' => true,
    }.each { |k, v|
      test "accepts #{k} parameter" do
        conf = parse_text(<<-EOS)
          <system>
            #{k} #{v}
          </system>
        EOS
        s = FakeSupervisor.new
        sc = Fluent::Supervisor::SystemConfig.new(conf)
        sc.apply(s)
        assert_not_nil(sc.instance_variable_get("@#{k}"))
        key = (k == 'emit_error_log_interval' ? 'suppress_interval' : k)
        assert_not_nil(s.instance_variable_get("@#{key}"))
      end
    }

    {'foo' => 'bar', 'hoge' => 'fuga'}.each { |k, v|
      test "should not affect settable parameters with unknown #{k} parameter" do
        s = FakeSupervisor.new
        sc = Fluent::Supervisor::SystemConfig.new({k => v})
        sc.apply(s)
        assert_nil(s.instance_variable_get(:@log_level))
        assert_nil(s.instance_variable_get(:@suppress_repeated_stacktrace))
        assert_nil(s.instance_variable_get(:@emit_error_log_interval))
        assert_nil(s.instance_variable_get(:@suppress_config_dump))
        assert_nil(s.instance_variable_get(:@without_source))
      end
    }

    test 'log_level' do
      conf = parse_text(<<-EOS)
        <system>
          log_level warn
        </system>
      EOS
      s = FakeSupervisor.new
      sc = Fluent::Supervisor::SystemConfig.new(conf)
      sc.apply(s)
      assert_equal(Fluent::Log::LEVEL_WARN, s.instance_variable_get("@log").level)
    end
  end
end
