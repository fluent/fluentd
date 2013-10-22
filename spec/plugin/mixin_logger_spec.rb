require 'fluentd/plugin_spec_helper'
require 'fluentd/plugin/mixin_logger'

include Fluentd::PluginSpecHelper

describe Fluentd::Plugin::LoggerMixin do
  let(:default_config) {
    %[]
  }

  def create_driver(conf = default_config)
    generate_driver(Fluentd::Plugin::Input, conf)
  end

  it 'test default configuration' do
    d = create_driver
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_DEBUG)
  end

  it 'test configure log_level' do
    d = create_driver(default_config + "\nlog_level fatal")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_FATAL)
    d = create_driver(default_config + "\nlog_level error")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_ERROR)
    d = create_driver(default_config + "\nlog_level warn")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_WARN)
    d = create_driver(default_config + "\nlog_level info")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_INFO)
    d = create_driver(default_config + "\nlog_level debug")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_DEBUG)
    d = create_driver(default_config + "\nlog_level trace")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_TRACE)

    d = create_driver(default_config + "\nlog_level 4")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_FATAL)
    d = create_driver(default_config + "\nlog_level 3")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_ERROR)
    d = create_driver(default_config + "\nlog_level 2")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_WARN)
    d = create_driver(default_config + "\nlog_level 1")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_INFO)
    d = create_driver(default_config + "\nlog_level 0")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_DEBUG)
    d = create_driver(default_config + "\nlog_level -1")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_TRACE)

    expect { create_driver(default_config + "\nlog_level foo") }.to raise_error(Fluentd::ConfigError)
  end

  it 'test mixin' do
    d = generate_driver(Fluentd::Plugin::Input, "log_level fatal")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_FATAL)
    d = generate_driver(Fluentd::Plugin::Output, "log_level fatal")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_FATAL)
    d = generate_driver(Fluentd::Plugin::Filter, "log_level fatal")
    expect(d.instance.logger.level).to eql(Fluentd::Logger::LEVEL_FATAL)
  end

end
