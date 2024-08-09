# frozen_string_literal: true

require_relative '../helper'
require 'flexmock/test_unit'
require 'fluent/plugin_helper/service_discovery'
require 'fluent/plugin/output'

class ServiceDiscoveryHelper < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :service_discovery

    # Make these mehtod public
    def service_discovery_create_manager(title, configurations:, load_balancer: nil, custom_build_method: nil, interval: 3)
      super
    end

    def discovery_manager
      super
    end
  end

  class DummyPlugin < Fluent::Plugin::TestBase
    helpers :service_discovery

    def configure(conf)
      super
      service_discovery_configure(:service_discovery_helper_test, static_default_service_directive: 'node')
    end

    def select_service(&block)
      service_discovery_select_service(&block)
    end

    # Make these mehtod public
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
      @d.after_shutdown unless @d.after_shutdown?
      @d.close unless @d.closed?
      @d.terminate unless @d.terminated?
    end
  end

  test 'support calling #service_discovery_create_manager and #discovery_manager from plugin' do
    d = @d = Dummy.new

    d.service_discovery_create_manager(
      :service_discovery_helper_test,
      configurations: [{ type: :static, conf: config_element('root', '', {}, [config_element('service', '', { 'host' => '127.0.0.1', 'port' => '1234' })]) }],
    )

    assert_true !!d.discovery_manager

    mock.proxy(d.discovery_manager).start.once
    mock.proxy(d).timer_execute(:service_discovery_helper_test, anything).never

    d.start
    d.event_loop_wait_until_start

    services = d.discovery_manager.services
    assert_equal 1, services.size
    assert_equal '127.0.0.1', services[0].host
    assert_equal 1234, services[0].port
  end

  test 'start discovery manager' do
    d = @d = DummyPlugin.new

    services = [config_element('service', '', { 'host' => '127.0.0.1', 'port' => '1234' })]
    d.configure(config_element('root', '', {}, [config_element('service_discovery', '', {'@type' => 'static'}, services)]))

    assert_true !!d.discovery_manager

    mock.proxy(d.discovery_manager).start.once
    mock.proxy(d).timer_execute(:service_discovery_helper_test, anything).never

    d.start
    d.event_loop_wait_until_start

    assert_equal 1, d.discovery_manager.services.size
    d.select_service do |serv|
      assert_equal "127.0.0.1", serv.host
      assert_equal 1234, serv.port
    end
  end

  test 'call timer_execute if dynamic configuration' do
    d = @d = DummyPlugin.new
    d.configure(config_element('root', '', {}, [config_element('service_discovery', '', { '@type' => 'file', 'path' => File.join(@sd_file_dir, 'config.yml' )})]))

    assert_true !!d.discovery_manager
    mock.proxy(d.discovery_manager).start.once
    mock(d).timer_execute(:service_discovery_helper_test, anything).once
    d.start
    d.event_loop_wait_until_start
  end

  test 'exits service discovery instances without any errors' do
    d = @d = DummyPlugin.new
    mockv = flexmock('dns_resolver', getaddress: '127.0.0.1')
              .should_receive(:getresources)
              .and_return([Resolv::DNS::Resource::IN::SRV.new(1, 10, 8081, 'service1.example.com')])
              .mock
    mock(Resolv::DNS).new { mockv }

    d.configure(config_element('root', '', {}, [config_element('service_discovery', '', { '@type' => 'srv', 'service' => 'service1', 'hostname' => 'example.com' })]))

    assert_true !!d.discovery_manager
    mock.proxy(d.discovery_manager).start.once
    mock(d).timer_execute(:service_discovery_helper_test, anything).once

    # To avoid claring `@logs` during `terminate` step
    # https://github.com/fluent/fluentd/blob/bc78d889f93dad8c2a4e0ad1ca802546185dacba/lib/fluent/test/log.rb#L33
    mock(d.log).reset.times(3)

    d.start
    d.event_loop_wait_until_start

    d.stop unless d.stopped?
    d.shutdown unless d.shutdown?
    d.after_shutdown unless d.after_shutdown?
    d.close unless d.closed?
    d.terminate unless d.terminated?

    assert_false(d.log.out.logs.any? { |e| e.match?(/thread doesn't exit correctly/) })
  end

  test 'static service discovery will be configured automatically when default service directive is specified' do
    d = @d = DummyPlugin.new

    nodes = [
      config_element('node', '', { 'host' => '192.168.0.1', 'port' => '24224' }),
      config_element('node', '', { 'host' => '192.168.0.2', 'port' => '24224' })
    ]
    d.configure(config_element('root', '', {}, nodes))

    assert_true !!d.discovery_manager

    mock.proxy(d.discovery_manager).start.once
    mock.proxy(d).timer_execute(:service_discovery_helper_test, anything).never

    d.start
    d.event_loop_wait_until_start

    assert_equal 2, d.discovery_manager.services.size
    d.select_service do |serv|
      assert_equal "192.168.0.1", serv.host
      assert_equal 24224, serv.port
    end
    d.select_service do |serv|
      assert_equal "192.168.0.2", serv.host
      assert_equal 24224, serv.port
    end
  end
end
