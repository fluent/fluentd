# frozen_string_literal: true

require_relative '../../helper'
require 'fluent/plugin_helper/service_discovery/manager'

class TestServiceDiscoveryManager < ::Test::Unit::TestCase
  setup do
    @sd_file_dir = File.expand_path('../../plugin/data/sd_file', __dir__)
  end

  class TestSdPlugin < Fluent::Plugin::ServiceDiscovery
    Fluent::Plugin.register_sd('test_sd', self)

    def initialize
      super
    end

    def service_in(host, port)
      s = Fluent::Plugin::ServiceDiscovery::Service.new(:sd_test, host, port)
      @queue << Fluent::Plugin::ServiceDiscovery.service_in_msg(s)
    end

    def service_out(host, port)
      s = Fluent::Plugin::ServiceDiscovery::Service.new(:sd_test, host, port)
      @queue << Fluent::Plugin::ServiceDiscovery.service_out_msg(s)
    end

    def start(queue)
      @queue = queue

      super
    end
  end

  sub_test_case '#configure' do
    test 'build sd plugins and services' do
      sdm = Fluent::PluginHelper::ServiceDiscovery::Manager.new(log: $log)
      sdm.configure(
        [
          { type: :file, conf: config_element('service_discovery', '', { 'path' => File.join(@sd_file_dir, 'config.yml') }) },
          { type: :static, conf: config_element('root', '', {}, [config_element('service', '', { 'host' => '127.0.0.2', 'port' => '5432' })]) },
        ],
      )

      assert_equal 3, sdm.services.size
      assert_equal 24224, sdm.services[0].port
      assert_equal '127.0.0.1', sdm.services[0].host

      assert_equal 24225, sdm.services[1].port
      assert_equal '127.0.0.1', sdm.services[1].host

      assert_equal 5432, sdm.services[2].port
      assert_equal '127.0.0.2', sdm.services[2].host

      assert_false sdm.static_config?
    end

    test 'no need to timer if only static' do
      sdm = Fluent::PluginHelper::ServiceDiscovery::Manager.new(log: $log)
      sdm.configure(
        [{ type: :static, conf: config_element('root', '', {}, [config_element('service', '', { 'host' => '127.0.0.2', 'port' => '5432' })]) }]
      )

      assert_equal 1, sdm.services.size
      assert_equal 5432, sdm.services[0].port
      assert_equal '127.0.0.2', sdm.services[0].host

      assert_true sdm.static_config?
    end
  end

  sub_test_case '#run_once' do
    test 'if new service added and deleted' do
      sdm = Fluent::PluginHelper::ServiceDiscovery::Manager.new(log: $log)
      t = TestSdPlugin.new
      mock(Fluent::Plugin).new_sd(:sd_test, parent: anything) { t }
      sdm.configure([{ type: :sd_test, conf: config_element('service_discovery', '', {})}])
      sdm.start

      assert_equal 0, sdm.services.size

      t.service_in('127.0.0.1', '1234')

      sdm.run_once
      assert_equal 1, sdm.services.size
      assert_equal '127.0.0.1', sdm.services[0].host
      assert_equal '1234', sdm.services[0].port

      t.service_out('127.0.0.1', '1234')

      sdm.run_once
      assert_equal 0, sdm.services.size
    end
  end
end
