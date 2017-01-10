require_relative 'helper'

require 'fluent/plugin'
require 'fluent/plugin/input'
require 'fluent/plugin/filter'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'
require 'fluent/plugin/parser'
require 'fluent/plugin/formatter'
require 'fluent/plugin/storage'

class PluginTest < Test::Unit::TestCase
  class Dummy1Input < Fluent::Plugin::Input
    Fluent::Plugin.register_input('plugin_test_dummy1', self)
  end
  class Dummy2Input < Fluent::Plugin::Input
    Fluent::Plugin.register_input('plugin_test_dummy2', self)
    helpers :storage
    config_section :storage do
      config_set_default :@type, 'plugin_test_dummy1'
    end
    def multi_workers_ready?
      true
    end
  end
  class DummyFilter < Fluent::Plugin::Filter
    Fluent::Plugin.register_filter('plugin_test_dummy', self)
    helpers :parser, :formatter
    config_section :parse do
      config_set_default :@type, 'plugin_test_dummy'
    end
    config_section :format do
      config_set_default :@type, 'plugin_test_dummy'
    end
    def filter(tag, time, record)
      record
    end
  end
  class Dummy1Output < Fluent::Plugin::Output
    Fluent::Plugin.register_output('plugin_test_dummy1', self)
    def write(chunk)
      # drop
    end
  end
  class Dummy2Output < Fluent::Plugin::Output
    Fluent::Plugin.register_output('plugin_test_dummy2', self)
    config_section :buffer do
      config_set_default :@type, 'plugin_test_dummy1'
    end
    def multi_workers_ready?
      true
    end
    def write(chunk)
      # drop
    end
  end
  class Dummy1Buffer < Fluent::Plugin::Buffer
    Fluent::Plugin.register_buffer('plugin_test_dummy1', self)
  end
  class Dummy2Buffer < Fluent::Plugin::Buffer
    Fluent::Plugin.register_buffer('plugin_test_dummy2', self)
    def multi_workers_ready?
      false
    end
  end
  class DummyParser < Fluent::Plugin::Parser
    Fluent::Plugin.register_parser('plugin_test_dummy', self)
  end
  class DummyFormatter < Fluent::Plugin::Formatter
    Fluent::Plugin.register_formatter('plugin_test_dummy', self)
  end
  class Dummy1Storage < Fluent::Plugin::Storage
    Fluent::Plugin.register_storage('plugin_test_dummy1', self)
  end
  class Dummy2Storage < Fluent::Plugin::Storage
    Fluent::Plugin.register_storage('plugin_test_dummy2', self)
    def multi_workers_ready?
      false
    end
  end
  class DummyOwner < Fluent::Plugin::Base
    include Fluent::PluginId
    include Fluent::PluginLoggerMixin
  end
  class DummyEventRouter
    def emit(tag, time, record); end
    def emit_array(tag, array); end
    def emit_stream(tag, es); end
    def emit_error_event(tag, time, record, error); end
  end

  sub_test_case '#new_* methods' do
    data(
      input1: ['plugin_test_dummy1', Dummy1Input, :new_input],
      input2: ['plugin_test_dummy2', Dummy2Input, :new_input],
      filter: ['plugin_test_dummy', DummyFilter, :new_filter],
      output1: ['plugin_test_dummy1', Dummy1Output, :new_output],
      output2: ['plugin_test_dummy2', Dummy2Output, :new_output],
    )
    test 'retruns plugin instances of registered plugin classes' do |(type, klass, m)|
      instance = Fluent::Plugin.__send__(m, type)
      assert_kind_of klass, instance
    end

    data(
      buffer1: ['plugin_test_dummy1', Dummy1Buffer, :new_buffer],
      buffer2: ['plugin_test_dummy2', Dummy2Buffer, :new_buffer],
      parser: ['plugin_test_dummy', DummyParser, :new_parser],
      formatter: ['plugin_test_dummy', DummyFormatter, :new_formatter],
      storage1: ['plugin_test_dummy1', Dummy1Storage, :new_storage],
      storage2: ['plugin_test_dummy2', Dummy2Storage, :new_storage],
    )
    test 'returns plugin instances of registered owned plugin classes' do |(type, klass, m)|
      owner = DummyOwner.new
      instance = Fluent::Plugin.__send__(m, type, parent: owner)
      assert_kind_of klass, instance
    end

    data(
      input1: ['plugin_test_dummy1', Dummy1Input, :new_input, nil],
      input2: ['plugin_test_dummy2', Dummy2Input, :new_input, nil],
      filter: ['plugin_test_dummy', DummyFilter, :new_filter, nil],
      output1: ['plugin_test_dummy1', Dummy1Output, :new_output, nil],
      output2: ['plugin_test_dummy2', Dummy2Output, :new_output, nil],
      buffer1: ['plugin_test_dummy1', Dummy1Buffer, :new_buffer, {parent: DummyOwner.new}],
      buffer2: ['plugin_test_dummy2', Dummy2Buffer, :new_buffer, {parent: DummyOwner.new}],
      parser: ['plugin_test_dummy', DummyParser, :new_parser, {parent: DummyOwner.new}],
      formatter: ['plugin_test_dummy', DummyFormatter, :new_formatter, {parent: DummyOwner.new}],
      storage1: ['plugin_test_dummy1', Dummy1Storage, :new_storage, {parent: DummyOwner.new}],
      storage2: ['plugin_test_dummy2', Dummy2Storage, :new_storage, {parent: DummyOwner.new}],
    )
    test 'returns plugin instances which are extended by FeatureAvailabilityChecker module' do |(type, _, m, kwargs)|
      instance = if kwargs
                   Fluent::Plugin.__send__(m, type, **kwargs)
                 else
                   Fluent::Plugin.__send__(m, type)
                 end
      assert_kind_of Fluent::Plugin::FeatureAvailabilityChecker, instance
    end
  end

  sub_test_case 'with default system configuration' do
    data(
      input1: ['plugin_test_dummy1', Dummy1Input, :new_input, nil],
      input2: ['plugin_test_dummy2', Dummy2Input, :new_input, nil],
      filter: ['plugin_test_dummy', DummyFilter, :new_filter, nil],
      output1: ['plugin_test_dummy1', Dummy1Output, :new_output, nil],
      output2: ['plugin_test_dummy2', Dummy2Output, :new_output, nil],
      buffer1: ['plugin_test_dummy1', Dummy1Buffer, :new_buffer, {parent: DummyOwner.new}],
      buffer2: ['plugin_test_dummy2', Dummy2Buffer, :new_buffer, {parent: DummyOwner.new}],
      parser: ['plugin_test_dummy', DummyParser, :new_parser, {parent: DummyOwner.new}],
      formatter: ['plugin_test_dummy', DummyFormatter, :new_formatter, {parent: DummyOwner.new}],
      storage1: ['plugin_test_dummy1', Dummy1Storage, :new_storage, {parent: DummyOwner.new}],
      storage2: ['plugin_test_dummy2', Dummy2Storage, :new_storage, {parent: DummyOwner.new}],
    )
    test '#configure does not raise anything' do |(type, _, m, kwargs)|
      instance = if kwargs
                   Fluent::Plugin.__send__(m, type, **kwargs)
                 else
                   Fluent::Plugin.__send__(m, type)
                 end
      if instance.respond_to?(:context_router=)
        instance.context_router = DummyEventRouter.new
      end
      assert_nothing_raised do
        instance.configure(config_element())
      end
    end
  end

  sub_test_case 'with single worker configuration' do
    data(
      input1: ['plugin_test_dummy1', Dummy1Input, :new_input, nil],
      input2: ['plugin_test_dummy2', Dummy2Input, :new_input, nil],
      filter: ['plugin_test_dummy', DummyFilter, :new_filter, nil],
      output1: ['plugin_test_dummy1', Dummy1Output, :new_output, nil],
      output2: ['plugin_test_dummy2', Dummy2Output, :new_output, nil],
      buffer1: ['plugin_test_dummy1', Dummy1Buffer, :new_buffer, {parent: DummyOwner.new}],
      buffer2: ['plugin_test_dummy2', Dummy2Buffer, :new_buffer, {parent: DummyOwner.new}],
      parser: ['plugin_test_dummy', DummyParser, :new_parser, {parent: DummyOwner.new}],
      formatter: ['plugin_test_dummy', DummyFormatter, :new_formatter, {parent: DummyOwner.new}],
      storage1: ['plugin_test_dummy1', Dummy1Storage, :new_storage, {parent: DummyOwner.new}],
      storage2: ['plugin_test_dummy2', Dummy2Storage, :new_storage, {parent: DummyOwner.new}],
    )
    test '#configure does not raise anything' do |(type, _, m, kwargs)|
      instance = if kwargs
                   Fluent::Plugin.__send__(m, type, **kwargs)
                 else
                   Fluent::Plugin.__send__(m, type)
                 end
      if instance.respond_to?(:context_router=)
        instance.context_router = DummyEventRouter.new
      end
      assert_nothing_raised do
        instance.system_config_override('workers' => 1)
        instance.configure(config_element())
      end
    end
  end

  sub_test_case 'with multi workers configuration' do
    data(
      input1: ['plugin_test_dummy1', Dummy1Input, :new_input],
      output1: ['plugin_test_dummy1', Dummy1Output, :new_output],
    )
    test '#configure raise configuration error if plugins are not ready for multi workers' do |(type, klass, new_method)|
      conf = config_element()
      instance = Fluent::Plugin.__send__(new_method, type)
      if instance.respond_to?(:context_router=)
        instance.context_router = DummyEventRouter.new
      end
      assert_raise Fluent::ConfigError.new("Plugin '#{type}' does not support multi workers configuration (#{klass})") do
        instance.system_config_override('workers' => 3)
        instance.configure(conf)
      end
    end

    data(
      input2: ['plugin_test_dummy2', Dummy2Input, :new_input], # with Dummy1Storage
      filter: ['plugin_test_dummy', DummyFilter, :new_filter], # with DummyParser and DummyFormatter
      output2: ['plugin_test_dummy2', Dummy2Output, :new_output], # with Dummy1Buffer
    )
    test '#configure does not raise any errors if plugins and its owned plugins are ready for multi workers' do |(type, _klass, new_method)|
      conf = config_element()
      instance = Fluent::Plugin.__send__(new_method, type)
      if instance.respond_to?(:context_router=)
        instance.context_router = DummyEventRouter.new
      end
      assert_nothing_raised do
        instance.system_config_override('workers' => 3)
        instance.configure(conf)
      end
    end

    data(
      input2: ['plugin_test_dummy2', Dummy2Input, :new_input, 'storage', 'plugin_test_dummy2', Dummy2Storage],
      output2: ['plugin_test_dummy2', Dummy2Output, :new_output, 'buffer', 'plugin_test_dummy2', Dummy2Buffer],
    )
    test '#configure raise configuration error if configured owned plugins are not ready for multi workers' do |(type, _klass, new_method, subsection, subsection_type, problematic)|
      conf = config_element('root', '', {}, [config_element(subsection, '', {'@type' => subsection_type})])
      instance = Fluent::Plugin.__send__(new_method, type)
      if instance.respond_to?(:context_router=)
        instance.context_router = DummyEventRouter.new
      end
      assert_raise Fluent::ConfigError.new("Plugin '#{subsection_type}' does not support multi workers configuration (#{problematic})") do
        instance.system_config_override('workers' => 3)
        instance.configure(conf)
      end
    end
  end
end
