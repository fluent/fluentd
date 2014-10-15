require 'helper'
require 'fluent/configurable'
require 'fluent/config/element'
require 'fluent/config/section'
require 'fluent/supervisor'

module Fluent::Config
  class TestSystemConfig < ::Test::Unit::TestCase
    def parse_text(text)
      basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
      Fluent::Config.parse(text, '(test)', basepath, true).elements.find { |e| e.name == 'system' }
    end

    test 'should not override default configurations when no parameters' do
      conf = parse_text(<<EOS)
<system>
</system>
EOS
      sc = Fluent::Supervisor::SystemConfig.new(conf)
      assert_nil(sc.log_level)
      assert_nil(sc.suppress_repeated_stacktrace)
      assert_nil(sc.emit_error_log_interval)
      assert_nil(sc.suppress_config_dump)
      assert_nil(sc.without_source)
      assert_empty(sc.to_opt)
    end

    {'log_level' => 'error', 'suppress_repeated_stacktrace' => true, 'emit_error_log_interval' => 60, 'suppress_config_dump' => true, 'without_source' => true}.each { |k, v|
      test "accepts #{k} parameter" do
        conf = parse_text(<<EOS)
<system>
  #{k} #{v}
</system>
EOS
        sc = Fluent::Supervisor::SystemConfig.new(conf)
        assert_not_nil(sc.instance_variable_get("@#{k}"))
        key = (k == 'emit_error_log_interval' ? :suppress_interval : k.to_sym)
        assert_include(sc.to_opt, key)
      end
    }

    {'foo' => 'bar', 'hoge' => 'fuga'}.each { |k, v|
      test "should not affect settable parameters with unknown #{k} parameter" do
        sc = Fluent::Supervisor::SystemConfig.new({k => v})
        assert_empty(sc.to_opt)
      end
    }
  end
end
