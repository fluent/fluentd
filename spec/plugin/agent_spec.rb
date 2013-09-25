require 'fluentd/plugin_spec_helper'
require 'fluentd/plugin/agent'

include Fluentd::PluginSpecHelper

describe Fluentd::Plugin::Agent do
  let(:default_config) {
    %[]
  }

  def create_driver(conf = default_config)
    generate_driver(Fluentd::Plugin::Agent, conf)
  end

  it 'test default config' do
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
end
