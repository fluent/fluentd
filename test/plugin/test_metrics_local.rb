require_relative '../helper'
require 'fluent/plugin/metrics_local'
require 'fluent/system_config'

class LocalMetricsTest < ::Test::Unit::TestCase
  sub_test_case 'configure' do
    test "configured for counter mode" do
      m = Fluent::Plugin::LocalMetrics.new
      m.configure(config_element('metrics', '', {"labels" => {test: "test-unit", language: "Ruby"}}))

      assert_false m.use_gauge_metric
      assert_equal({agent: "Fluentd", hostname: "#{Socket.gethostname}"}, m.default_labels)
      assert_equal({test: "test-unit", language: "Ruby"}, m.labels)
      assert_true m.has_methods_for_counter
      assert_false m.has_methods_for_gauge
    end

    test "configured for gauge mode" do
      m = Fluent::Plugin::LocalMetrics.new
      m.use_gauge_metric = true
      m.configure(config_element('metrics', '', {"labels" => {test: "test-unit", language: "Ruby"}}))

      assert_true m.use_gauge_metric
      assert_equal({agent: "Fluentd", hostname: "#{Socket.gethostname}"}, m.default_labels)
      assert_equal({test: "test-unit", language: "Ruby"}, m.labels)
      assert_false m.has_methods_for_counter
      assert_true m.has_methods_for_gauge
    end
  end

  sub_test_case 'LocalMetric' do
    sub_test_case "counter" do
      setup do
        @m = Fluent::Plugin::LocalMetrics.new
        @m.configure(config_element('metrics', '', {}))
      end

      test '#configure' do
        assert_true @m.has_methods_for_counter
        assert_false @m.has_methods_for_gauge
      end

      test 'all local counter operations work well' do
        assert_equal 0, @m.get('key')
        assert_equal 1, @m.inc('key')

        @m.add('key', 20)
        assert_equal 21, @m.get('key')
        assert_raise NotImplementedError do
          @m.dec('key')
        end

        @m.set('key', 100)
        assert_equal 100, @m.get('key')

        @m.set('key', 10)
        assert_equal 100, @m.get('key') # On counter, value should be overwritten bigger than stored one.
        assert_raise NotImplementedError do
          @m.sub('key', 11)
        end
      end
    end

    sub_test_case "gauge" do
      setup do
        @m = Fluent::Plugin::LocalMetrics.new
        @m.use_gauge_metric = true
        @m.configure(config_element('metrics', '', {}))
      end

      test '#configure' do
        assert_false @m.has_methods_for_counter
        assert_true @m.has_methods_for_gauge
      end

      test 'all local gauge operations work well' do
        assert_equal 0, @m.get('key')
        assert_equal 1, @m.inc('key')

        @m.add('key', 20)
        assert_equal 21, @m.get('key')
        @m.dec('key')
        assert_equal 20, @m.get('key')

        @m.set('key', 100)
        assert_equal 100, @m.get('key')

        @m.sub('key', 11)
        assert_equal 89, @m.get('key')

        @m.set('key', 10)
        assert_equal 10, @m.get('key') # On gauge, value always should be overwritten.
      end
    end
  end
end
