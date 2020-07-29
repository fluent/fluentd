require_relative 'helper'

require 'fluent/config'
require 'fluent/static_config_analysis'
require 'fluent/plugin/out_forward'
require 'fluent/plugin/out_stdout'
require 'fluent/plugin/out_exec'
require 'fluent/plugin/in_forward'
require 'fluent/plugin/in_sample'
require 'fluent/plugin/filter_grep'
require 'fluent/plugin/filter_stdout'
require 'fluent/plugin/filter_parser'

class StaticConfigAnalysisTest < ::Test::Unit::TestCase
  sub_test_case '.call' do
    test 'returns outputs, inputs and filters' do
      conf_data = <<-CONF
<source>
  @type forward
</source>
<filter>
  @type grep
</filter>
<match>
  @type forward
</match>
      CONF

      c = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
      ret = Fluent::StaticConfigAnalysis.call(c)
      assert_equal 1, ret.outputs.size
      assert_kind_of Fluent::Plugin::ForwardOutput, ret.outputs[0].plugin
      assert_equal 1, ret.inputs.size
      assert_kind_of Fluent::Plugin::ForwardInput, ret.inputs[0].plugin
      assert_equal 1, ret.filters.size
      assert_kind_of Fluent::Plugin::GrepFilter, ret.filters[0].plugin
      assert_empty ret.labels

      assert_equal [Fluent::Plugin::ForwardOutput, Fluent::Plugin::ForwardInput, Fluent::Plugin::GrepFilter], ret.all_plugins.map(&:class)
    end

    test 'returns wrapped element with worker and label section' do
      conf_data = <<-CONF
<source>
  @type forward
</source>
<filter>
  @type grep
</filter>
<match>
  @type forward
</match>
<worker 0>
  <source>
    @type dummy
  </source>
  <filter>
    @type parser
  </filter>
  <match>
    @type exec
  </match>
</worker>
<label @test>
  <filter>
    @type stdout
  </filter>
  <match>
    @type stdout
  </match>
</label>
      CONF

      c = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
      ret = Fluent::StaticConfigAnalysis.call(c)
      assert_equal [Fluent::Plugin::ExecOutput, Fluent::Plugin::StdoutOutput, Fluent::Plugin::ForwardOutput], ret.outputs.map(&:plugin).map(&:class)
      assert_equal [Fluent::Plugin::SampleInput, Fluent::Plugin::ForwardInput], ret.inputs.map(&:plugin).map(&:class)
      assert_equal [Fluent::Plugin::ParserFilter, Fluent::Plugin::StdoutFilter, Fluent::Plugin::GrepFilter], ret.filters.map(&:plugin).map(&:class)
      assert_equal 1, ret.labels.size
      assert_equal '@test', ret.labels[0].name
    end

    sub_test_case 'raises config error' do
      data(
        'empty' => ['', 'Missing worker id on <worker> directive'],
        'invalid number' => ['a', 'worker id should be integer: a'],
        'worker id is negative' => ['-1', 'worker id should be integer: -1'],
        'min worker id is less than 0' => ['-1-1', 'worker id should be integer: -1-1'],
        'max worker id is less than 0' => ['1--1', 'worker id -1 specified by <worker> directive is not allowed. Available worker id is between 0 and 1'],
        'min worker id is greater than workers' => ['0-2', 'worker id 2 specified by <worker> directive is not allowed. Available worker id is between 0 and 1'],
        'max worker is less than min worker' => ['1-0', "greater first_worker_id<1> than last_worker_id<0> specified by <worker> directive is not allowed. Available multi worker assign syntax is <smaller_worker_id>-<greater_worker_id>"],
      )
      test 'when worker number is invalid' do |v|
        val, msg = v
        conf_data = <<-CONF
<worker #{val}>
</worker>
CONF

        c = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
        assert_raise(Fluent::ConfigError.new(msg)) do
          Fluent::StaticConfigAnalysis.call(c, workers: 2)
        end
      end

      test 'when worker number is duplicated' do
        conf_data = <<-CONF
<worker 0-1>
</worker>
<worker 0-1>
</worker>
CONF

        c = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
        assert_raise(Fluent::ConfigError.new("specified worker_id<0> collisions is detected on <worker> directive. Available worker id(s): []")) do
          Fluent::StaticConfigAnalysis.call(c, workers: 2)
        end
      end

      test 'duplicated label exits' do
        conf_data = <<-CONF
<label @dup>
</label>
<label @dup>
</label>
CONF

        c = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
        assert_raise(Fluent::ConfigError.new('Section <label @dup> appears twice')) do
          Fluent::StaticConfigAnalysis.call(c, workers: 2)
        end
      end

      test 'empty label' do
        conf_data = <<-CONF
<label>
</label>
CONF

        c = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
        assert_raise(Fluent::ConfigError.new('Missing symbol argument on <label> directive')) do
          Fluent::StaticConfigAnalysis.call(c, workers: 2)
        end
      end

      data(
        'in filter' => 'filter',
        'in source' => 'source',
        'in match' => 'match',
      )
      test 'when @type is missing' do |name|
        conf_data = <<-CONF
<#{name}>
  @type
</#{name}>
CONF
        c = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
        assert_raise(Fluent::ConfigError.new("Missing '@type' parameter on <#{name}> directive")) do
          Fluent::StaticConfigAnalysis.call(c)
        end
      end

      test 'when worker has worker section' do
        conf_data = <<-CONF
<worker 0>
  <worker 0>
  </worker>
</worker>
CONF
        c = Fluent::Config.parse(conf_data, '(test)', '(test_dir)', true)
        assert_raise(Fluent::ConfigError.new("<worker> section cannot have <worker> directive")) do
          Fluent::StaticConfigAnalysis.call(c)
        end
      end
    end
  end
end
