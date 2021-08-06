require_relative '../helper'
require 'fluent/plugin_helper/metrics'
require 'fluent/plugin/base'

class MetricsTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :metrics
    def configure(conf)
      super
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

  test 'can be initialized without any metrics at first' do
    d = Dummy.new
    assert_equal 0, d._metrics.size
  end

  test 'can be configured' do
    d1 = Dummy.new
    assert_nothing_raised do
      d1.configure(config_element())
    end
    assert d1.plugin_id
    assert d1.log
  end

  test 'creates metrics instances' do
    d = Dummy.new
    i = d.metrics_create(namespace: "fluentd_test", subsystem: "unit-test", name: "metrics1", help_text: "metrics testing")
    d.configure(config_element())
    assert do
      d.instance_variable_get(:@plugin_type_or_id).include?("dummy.object")
    end
    assert{ i.is_a?(Fluent::Plugin::LocalMetrics) }
    assert_true i.has_methods_for_counter
    assert_false i.has_methods_for_gauge

    d = Dummy.new
    i = d.metrics_create(namespace: "fluentd_test", subsystem: "unit-test", name: "metrics2", help_text: "metrics testing", prefer_gauge: true)
    d.configure(config_element())
    assert do
      d.instance_variable_get(:@plugin_type_or_id).include?("dummy.object")
    end
    assert{ i.is_a?(Fluent::Plugin::LocalMetrics) }
    assert_false i.has_methods_for_counter
    assert_true i.has_methods_for_gauge
  end

  test 'calls lifecycle methods for all plugin instances via owner plugin' do
    @d = d = Dummy.new
    i1 = d.metrics_create(namespace: "fluentd_test", subsystem: "unit-test", name: "metrics1", help_text: "metrics testing")
    i2 = d.metrics_create(namespace: "fluentd_test", subsystem: "unit-test", name: "metrics2", help_text: "metrics testing", prefer_gauge: true)
    i3 = d.metrics_create(namespace: "fluentd_test", subsystem: "unit-test", name: "metrics3", help_text: "metrics testing")
    d.configure(config_element())
    assert do
      d.instance_variable_get(:@plugin_type_or_id).include?("dummy.object")
    end
    d.start

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
