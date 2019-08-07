require_relative '../helper'
require 'flexmock/test_unit'
require 'fluent/plugin_helper/service_discovery'
require 'fluent/plugin/output'

class ServiceDiscoveryHelper < Test::Unit::TestCase
  PORT = unused_port
  NULL_LOGGER = Logger.new(nil)

  class Dummy < Fluent::Plugin::TestBase
    helpers :service_discovery

    # Make these mehtod public
    def create_service_discovery_manager(title, configurations:, load_balancer: nil, custom_build_method: nil, interval: 3)
      super
    end

    def discovery_manager
      super
    end
  end

  setup do
    @sd_file_dir = File.expand_path('../plugin/data/sd_file', __dir__)

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

  test 'start discovery manager' do
    d = @d = Dummy.new

    d.create_service_discovery_manager(
      :service_discovery_helper_test,
      configurations: [{ type: :static, conf: config_element('server', '', { 'host' => '127.0.0.1', 'port' => '1234' }) }],
    )

    assert_true !!d.discovery_manager

    mock.proxy(d.discovery_manager).start.once
    mock.proxy(d).timer_execute(:service_discovery_helper_test, anything).never

    d.start

    services = d.discovery_manager.services
    assert_equal 1, services.size
    assert_equal '127.0.0.1', services[0].host
    assert_equal 1234, services[0].port
  end

  test 'call timer_execute if dynamic configuration' do
    d = @d = Dummy.new

    d.create_service_discovery_manager(
      :service_discovery_helper_test,
      configurations: [{ type: :file, conf: config_element('server', '', { 'path' => File.join(@sd_file_dir, 'config.yml') }) }],
    )

    assert_true !!d.discovery_manager
    mock.proxy(d.discovery_manager).start.once
    mock(d).timer_execute(:service_discovery_helper_test, anything).once
    d.start
  end
end
