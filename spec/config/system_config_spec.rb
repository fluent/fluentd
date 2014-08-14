require 'fluent/configurable'
require 'fluent/config/element'
require 'fluent/config/section'
require 'fluent/supervisor'

describe Fluent::Supervisor::SystemConfig do
  def parse_text(text)
    basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
    Fluent::Config.parse(text, '(test)', basepath, true).elements.find { |e| e.name == 'system' }
  end

  it 'should not override default configurations when no parameters' do
    conf = parse_text(<<EOS)
<system>
</system>
EOS
    sc = Fluent::Supervisor::SystemConfig.new(conf)
    expect(sc.log_level).to be_nil
    expect(sc.suppress_repeated_stacktrace).to be_nil
    expect(sc.emit_error_log_interval).to be_nil
    expect(sc.suppress_config_dump).to be_nil
    expect(sc.without_source).to be_nil
    expect(sc.to_opt).to eql({})
  end

  {'log_level' => 'error', 'suppress_repeated_stacktrace' => true, 'emit_error_log_interval' => 60, 'suppress_config_dump' => true, 'without_source' => true}.each { |k, v|
    it "accepts #{k} parameter" do
      conf = parse_text(<<EOS)
<system>
  #{k} #{v}
</system>
EOS
      sc = Fluent::Supervisor::SystemConfig.new(conf)
      expect(sc.instance_variable_get("@#{k}")).not_to be_nil
      if k == 'emit_error_log_interval'
        expect(sc.to_opt).to include(:suppress_interval)
      else
        expect(sc.to_opt).to include(k.to_sym)
      end
    end
  }

  {'foo' => 'bar', 'hoge' => 'fuga'}.each { |k, v|
    it "should not affect settable parameters with unknown #{k} parameter" do
      sc = Fluent::Supervisor::SystemConfig.new({k => v})
      expect(sc.to_opt).to be_empty
    end
  }
end
