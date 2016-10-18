require_relative '../helper'
require 'fluent/plugin_helper/formatter'
require 'fluent/plugin/base'

class FormatterHelperTest < Test::Unit::TestCase
  class ExampleFormatter < Fluent::Plugin::Formatter
    Fluent::Plugin.register_formatter('example', self)
    def format(tag, time, record)
      "#{tag},#{time.to_i},#{record.keys.sort.join(',')}" # hey, you miss values! :P
    end
  end
  class Example2Formatter < Fluent::Plugin::Formatter
    Fluent::Plugin.register_formatter('example2', self)
    def format(tag, time, record)
      "#{tag},#{time.to_i},#{record.values.sort.join(',')}" # key...
    end
  end
  class Dummy < Fluent::Plugin::TestBase
    helpers :formatter
    config_section :format do
      config_set_default :@type, 'example'
    end
  end

  class Dummy2 < Fluent::Plugin::TestBase
    helpers :formatter
    config_section :format do
      config_set_default :@type, 'example2'
    end
  end

  setup do
    @d = nil
  end

  teardown do
    if @d
      @d.stop unless @d.stopped?
      @d.shutdown unless @d.shutdown?
      @d.close unless @d.closed?
      @d.terminate unless @d.terminated?
    end
  end

  test 'can be initialized without any formatters at first' do
    d = Dummy.new
    assert_equal 0, d._formatters.size
  end

  test 'can override default configuration parameters, but not overwrite whole definition' do
    d = Dummy.new
    assert_equal [], d.formatter_configs

    d = Dummy2.new
    d.configure(config_element('ROOT', '', {}, [config_element('format', '', {}, [])]))
    assert_raise NoMethodError do
      d.format
    end
    assert_equal 1, d.formatter_configs.size
    assert_equal 'example2', d.formatter_configs.first[:@type]
  end

  test 'creates instance of type specified by conf, or default_type if @type is missing in conf' do
    d = Dummy2.new
    d.configure(config_element())
    i = d.formatter_create(conf: config_element('format', '', {'@type' => 'example'}), default_type: 'example2')
    assert{ i.is_a?(ExampleFormatter) }

    d = Dummy2.new
    d.configure(config_element())
    i = d.formatter_create(conf: nil, default_type: 'example2')
    assert{ i.is_a?(Example2Formatter) }
  end

  test 'raises config error if config section is specified, but @type is not specified' do
    d = Dummy2.new
    d.configure(config_element())
    assert_raise Fluent::ConfigError.new("@type is required in <format>") do
      d.formatter_create(conf: config_element('format', '', {}), default_type: 'example2')
    end
  end

  test 'can be configured with default type without format sections' do
    d = Dummy.new
    assert_nothing_raised do
      d.configure(config_element())
    end
    assert_equal 1, d._formatters.size
  end

  test 'can be configured with a format section' do
    d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('format', '', {'@type' => 'example'})
      ])
    assert_nothing_raised do
      d.configure(conf)
    end
    assert_equal 1, d._formatters.size
    assert{ d._formatters.values.all?{ |formatter| !formatter.started? } }
  end

  test 'can be configured with 2 or more format sections with different usages with each other' do
    d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('format', 'default', {'@type' => 'example'}),
        config_element('format', 'extra', {'@type' => 'example2'}),
      ])
    assert_nothing_raised do
      d.configure(conf)
    end
    assert_equal 2, d._formatters.size
    assert{ d._formatters.values.all?{ |formatter| !formatter.started? } }
  end

  test 'cannot be configured with 2 format sections with same usage' do
    d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('format', 'default', {'@type' => 'example'}),
        config_element('format', 'extra', {'@type' => 'example2'}),
        config_element('format', 'extra', {'@type' => 'example2'}),
      ])
    assert_raises Fluent::ConfigError do
      d.configure(conf)
    end
  end

  test 'creates a format plugin instance which is already configured without usage' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('format', '', {'@type' => 'example'})
      ])
    d.configure(conf)
    d.start

    formatter = d.formatter_create
    assert{ formatter.is_a? ExampleFormatter }
    assert formatter.started?
  end

  test 'creates a formatter plugin instance which is already configured with usage' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('format', 'mydata', {'@type' => 'example'})
      ])
    d.configure(conf)
    d.start

    formatter = d.formatter_create(usage: 'mydata')
    assert{ formatter.is_a? ExampleFormatter }
    assert formatter.started?
  end

  test 'creates a formatter plugin without configurations' do
    @d = d = Dummy.new
    d.configure(config_element())
    d.start

    formatter = d.formatter_create(usage: 'mydata', type: 'example', conf: config_element('format', 'mydata'))
    assert{ formatter.is_a? ExampleFormatter }
    assert formatter.started?
  end

  test 'creates 2 or more formatter plugin instances' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('format', 'mydata', {'@type' => 'example'}),
        config_element('format', 'secret', {'@type' => 'example2'})
      ])
    d.configure(conf)
    d.start

    p1 = d.formatter_create(usage: 'mydata')
    p2 = d.formatter_create(usage: 'secret')
    assert{ p1.is_a? ExampleFormatter }
    assert p1.started?
    assert{ p2.is_a? Example2Formatter }
    assert p2.started?
  end

  test 'calls lifecycle methods for all plugin instances via owner plugin' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [ config_element('format', '', {'@type' => 'example'}), config_element('format', 'e2', {'@type' => 'example'}) ])
    d.configure(conf)
    d.start

    i1 = d.formatter_create(usage: '')
    i2 = d.formatter_create(usage: 'e2')
    i3 = d.formatter_create(usage: 'e3', type: 'example2')

    assert i1.started?
    assert i2.started?
    assert i3.started?

    assert !i1.stopped?
    assert !i2.stopped?
    assert !i3.stopped?

    d.stop

    assert i1.stopped?
    assert i2.stopped?
    assert i3.stopped?

    assert !i1.before_shutdown?
    assert !i2.before_shutdown?
    assert !i3.before_shutdown?

    d.before_shutdown

    assert i1.before_shutdown?
    assert i2.before_shutdown?
    assert i3.before_shutdown?

    assert !i1.shutdown?
    assert !i2.shutdown?
    assert !i3.shutdown?

    d.shutdown

    assert i1.shutdown?
    assert i2.shutdown?
    assert i3.shutdown?

    assert !i1.after_shutdown?
    assert !i2.after_shutdown?
    assert !i3.after_shutdown?

    d.after_shutdown

    assert i1.after_shutdown?
    assert i2.after_shutdown?
    assert i3.after_shutdown?

    assert !i1.closed?
    assert !i2.closed?
    assert !i3.closed?

    d.close

    assert i1.closed?
    assert i2.closed?
    assert i3.closed?

    assert !i1.terminated?
    assert !i2.terminated?
    assert !i3.terminated?

    d.terminate

    assert i1.terminated?
    assert i2.terminated?
    assert i3.terminated?
  end
end
