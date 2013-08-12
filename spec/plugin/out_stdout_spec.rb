require 'fluentd/plugin_spec_helper'
require 'fluentd/plugin/out_stdout'

include Fluentd::PluginSpecHelper

describe Fluentd::Plugin::StdoutOutput do
  let(:default_config) {
    %[]
  }

  def create_driver(conf = default_config)
    generate_driver(Fluentd::Plugin::StdoutOutput, conf)
  end

  it 'test configure' do
    d = create_driver
    expect(d.instance.output_type).to eql(:json)
  end

  it 'test configure output type' do
    d = create_driver(default_config + "\noutput_type json")
    expect(d.instance.output_type).to eql(:json)

    d = create_driver(default_config + "\noutput_type hash")
    expect(d.instance.output_type).to eql(:hash)

    expect { create_driver(default_config + "\noutput_type foo") }.to raise_error(Fluentd::ConfigError)
  end

  it 'test emit json' do
    d = create_driver(default_config + "\noutput_type json")
    time = Time.now
    out = capture_stdout { d.pitch('test', time.to_i, {'test' => 'test'}) }
    expect(out).to eql("#{time.localtime} test: {\"test\":\"test\"}\n")

    # NOTE: Float::NAN is not jsonable
    expect { d.pitch('test', time.to_i, {'test' => Float::NAN}) }.to raise_error(Yajl::EncodeError)
  end

  it 'test emit hash' do
    d = create_driver(default_config + "\noutput_type hash")
    time = Time.now
    out = capture_stdout { d.pitch('test', time.to_i, {'test' => 'test'}) }
    expect(out).to eql("#{time.localtime} test: {\"test\"=>\"test\"}\n")

    # NOTE: Float::NAN is not jsonable, but hash string can output it.
    out = capture_stdout { d.pitch('test', time.to_i, {'test' => Float::NAN}) }
    expect(out).to eql("#{time.localtime} test: {\"test\"=>NaN}\n")
  end

  private

  # Capture the stdout of the block given
  def capture_stdout(&block)
    out = StringIO.new
    $stdout = out
    yield
    return out.string
  ensure
    $stdout = STDOUT
  end
end

