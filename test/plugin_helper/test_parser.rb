require_relative '../helper'
require 'fluent/plugin_helper/parser'
require 'fluent/plugin/base'
require 'fluent/time'

class ParserHelperTest < Test::Unit::TestCase
  class ExampleParser < Fluent::Plugin::Parser
    Fluent::Plugin.register_parser('example', self)
    def parse(text)
      ary = text.split(/\s*,\s*/)
      r = {}
      ary.each_with_index do |v, i|
        r[i.to_s] = v
      end
      yield Fluent::EventTime.now, r
    end
  end
  class Example2Parser < Fluent::Plugin::Parser
    Fluent::Plugin.register_parser('example2', self)
    def parse(text)
      ary = text.split(/\s*,\s*/)
      r = {}
      ary.each_with_index do |v, i|
        r[i.to_s] = v
      end
      yield Fluent::EventTime.now, r
    end
  end
  class Dummy < Fluent::Plugin::TestBase
    helpers :parser
    config_section :parse do
      config_set_default :@type, 'example'
    end
  end

  class Dummy2 < Fluent::Plugin::TestBase
    helpers :parser
    config_section :parse do
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

  test 'can be initialized without any parsers at first' do
    d = Dummy.new
    assert_equal 0, d._parsers.size
  end

  test 'can override default configuration parameters, but not overwrite whole definition' do
    d = Dummy.new
    assert_equal [], d.parser_configs

    d = Dummy2.new
    d.configure(config_element('ROOT', '', {}, [config_element('parse', '', {}, [])]))
    assert_raise NoMethodError do
      d.parse
    end
    assert_equal 1, d.parser_configs.size
    assert_equal 'example2', d.parser_configs.first[:@type]
  end

  test 'creates instance of type specified by conf, or default_type if @type is missing in conf' do
    d = Dummy2.new
    d.configure(config_element())
    i = d.parser_create(conf: config_element('parse', '', {'@type' => 'example'}), default_type: 'example2')
    assert{ i.is_a?(ExampleParser) }

    d = Dummy2.new
    d.configure(config_element())
    i = d.parser_create(conf: nil, default_type: 'example2')
    assert{ i.is_a?(Example2Parser) }
  end

  test 'raises config error if config section is specified, but @type is not specified' do
    d = Dummy2.new
    d.configure(config_element())
    assert_raise Fluent::ConfigError.new("@type is required in <parse>") do
      d.parser_create(conf: config_element('parse', '', {}), default_type: 'example2')
    end
  end

  test 'can be configured with default type without parse sections' do
    d = Dummy.new
    d.configure(config_element())
    assert_equal 1, d._parsers.size
  end

  test 'can be configured with a parse section' do
    d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('parse', '', {'@type' => 'example'})
      ])
    assert_nothing_raised do
      d.configure(conf)
    end
    assert_equal 1, d._parsers.size
    assert{ d._parsers.values.all?{ |parser| !parser.started? } }
  end

  test 'can be configured with 2 or more parse sections with different usages with each other' do
    d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('parse', 'default', {'@type' => 'example'}),
        config_element('parse', 'extra', {'@type' => 'example2'}),
      ])
    assert_nothing_raised do
      d.configure(conf)
    end
    assert_equal 2, d._parsers.size
    assert{ d._parsers.values.all?{ |parser| !parser.started? } }
  end

  test 'cannot be configured with 2 parse sections with same usage' do
    d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('parse', 'default', {'@type' => 'example'}),
        config_element('parse', 'extra', {'@type' => 'example2'}),
        config_element('parse', 'extra', {'@type' => 'example2'}),
      ])
    assert_raises Fluent::ConfigError do
      d.configure(conf)
    end
  end

  test 'creates a parse plugin instance which is already configured without usage' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('parse', '', {'@type' => 'example'})
      ])
    d.configure(conf)
    d.start

    parser = d.parser_create
    assert{ parser.is_a? ExampleParser }
    assert parser.started?
  end

  test 'creates a parser plugin instance which is already configured with usage' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('parse', 'mydata', {'@type' => 'example'})
      ])
    d.configure(conf)
    d.start

    parser = d.parser_create(usage: 'mydata')
    assert{ parser.is_a? ExampleParser }
    assert parser.started?
  end

  test 'creates a parser plugin without configurations' do
    @d = d = Dummy.new
    d.configure(config_element())
    d.start

    parser = d.parser_create(usage: 'mydata', type: 'example', conf: config_element('parse', 'mydata'))
    assert{ parser.is_a? ExampleParser }
    assert parser.started?
  end

  test 'creates 2 or more parser plugin instances' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [
        config_element('parse', 'mydata', {'@type' => 'example'}),
        config_element('parse', 'secret', {'@type' => 'example2'})
      ])
    d.configure(conf)
    d.start

    p1 = d.parser_create(usage: 'mydata')
    p2 = d.parser_create(usage: 'secret')
    assert{ p1.is_a? ExampleParser }
    assert p1.started?
    assert{ p2.is_a? Example2Parser }
    assert p2.started?
  end

  test 'calls lifecycle methods for all plugin instances via owner plugin' do
    @d = d = Dummy.new
    conf = config_element('ROOT', '', {}, [ config_element('parse', '', {'@type' => 'example'}), config_element('parse', 'e2', {'@type' => 'example'}) ])
    d.configure(conf)
    d.start

    i1 = d.parser_create(usage: '')
    i2 = d.parser_create(usage: 'e2')
    i3 = d.parser_create(usage: 'e3', type: 'example2')

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
