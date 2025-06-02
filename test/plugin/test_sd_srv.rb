require_relative '../helper'
require 'fluent/plugin/sd_srv'
require 'fileutils'
require 'flexmock/test_unit'
require 'json'

class SrvServiceDiscoveryTest < ::Test::Unit::TestCase
  SRV_RECORD1 = Resolv::DNS::Resource::IN::SRV.new(1, 10, 8081, 'service1.example.com')
  SRV_RECORD2 = Resolv::DNS::Resource::IN::SRV.new(2, 20, 8082, 'service2.example.com')

  sub_test_case 'configure' do
    test 'set services ordered by priority' do
      sdf = Fluent::Plugin::SrvServiceDiscovery.new
      mock(Resolv::DNS).new { flexmock('dns_resolver', getresources: [SRV_RECORD2, SRV_RECORD1], getaddress: '127.0.0.1') }

      sdf.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com' }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8081, 'service1.example.com', 10, false, '', '', nil), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8082, 'service2.example.com', 20, false, '', '', nil), sdf.services[1]
    end

    test 'return host name without resolving name when dns_lookup is false' do
      sdf = Fluent::Plugin::SrvServiceDiscovery.new
      mock(Resolv::DNS).new { flexmock('dns_resolver', getresources: [SRV_RECORD1, SRV_RECORD2], getaddress: '127.0.0.1') }

      sdf.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com', 'dns_lookup' => false }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, 'service1.example.com', 8081, 'service1.example.com', 10, false, '', '', nil), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, 'service2.example.com', 8082, 'service2.example.com', 20, false, '', '', nil), sdf.services[1]
    end

    test 'pass a value as :nameserver to Resolve::DNS when dns_server_host is given' do
      sdf = Fluent::Plugin::SrvServiceDiscovery.new
      mock(Resolv::DNS).new(nameserver: '8.8.8.8') { flexmock('dns_resolver', getresources: [SRV_RECORD1, SRV_RECORD2], getaddress: '127.0.0.1') }

      sdf.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com', 'dns_server_host' => '8.8.8.8' }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8081, 'service1.example.com', 10, false, '', '', nil), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8082, 'service2.example.com', 20, false, '', '', nil), sdf.services[1]
    end

    test 'pass a value as :nameserver_port to Resolve::DNS when dns_server_host has port' do
      sdf = Fluent::Plugin::SrvServiceDiscovery.new
      mock(Resolv::DNS).new(nameserver_port: [['8.8.8.8', 8080]]) { flexmock('dns_resolver', getresources: [SRV_RECORD1, SRV_RECORD2], getaddress: '127.0.0.1') }

      sdf.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com', 'dns_server_host' => '8.8.8.8:8080' }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8081, 'service1.example.com', 10, false, '', '', nil), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8082, 'service2.example.com', 20, false, '', '', nil), sdf.services[1]
    end

    test 'target follows RFC2782' do
      sdf = Fluent::Plugin::SrvServiceDiscovery.new
      mock = flexmock('dns_resolver', getaddress: '127.0.0.1')
               .should_receive(:getresources).with("_service1._tcp.example.com", Resolv::DNS::Resource::IN::SRV)
               .and_return([SRV_RECORD1, SRV_RECORD2])
               .mock

      mock(Resolv::DNS).new { mock }
      sdf.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com' }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8081, 'service1.example.com', 10, false, '', '', nil), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8082, 'service2.example.com', 20, false, '', '', nil), sdf.services[1]
    end

    test 'can change protocol' do
      sdf = Fluent::Plugin::SrvServiceDiscovery.new
      mock = flexmock('dns_resolver', getaddress: '127.0.0.1')
               .should_receive(:getresources).with("_service1._udp.example.com", Resolv::DNS::Resource::IN::SRV)
               .and_return([SRV_RECORD1, SRV_RECORD2])
               .mock

      mock(Resolv::DNS).new { mock }
      sdf.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com', 'proto' => 'udp' }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8081, 'service1.example.com', 10, false, '', '', nil), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8082, 'service2.example.com', 20, false, '', '', nil), sdf.services[1]
    end

    test 'can set password, username, password' do
      sdf = Fluent::Plugin::SrvServiceDiscovery.new
      mock(Resolv::DNS).new { flexmock('dns_resolver', getresources: [SRV_RECORD2, SRV_RECORD1], getaddress: '127.0.0.1') }

      sdf.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com', 'shared_key' => 'key', 'username' => 'user', 'password' => 'pass' }))
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8081, 'service1.example.com', 10, false, 'user', 'pass', 'key'), sdf.services[0]
      assert_equal Fluent::Plugin::ServiceDiscovery::Service.new(:srv, '127.0.0.1', 8082, 'service2.example.com', 20, false, 'user', 'pass', 'key'), sdf.services[1]
    end
  end

  sub_test_case '#start' do
    module TestTimerEventHelperWrapper
      # easy to control statsevent
      def timer_execute(_name, _interval, &block)
        @test_timer_event_helper_wrapper_context = Fiber.new do
          loop do
            block.call

            if Fiber.yield == :finish
              break
            end
          end
        end

        resume
      end

      def resume
        @test_timer_event_helper_wrapper_context.resume(:resume)
      end

      def shutdown
        super

        if @test_timer_event_helper_wrapper_context
          @test_timer_event_helper_wrapper_context.resume(:finish)
        end

      end
    end

    setup do
      sds = Fluent::Plugin::SrvServiceDiscovery.new
      @sd_srv = sds
    end

    teardown do
      if @sd_srv
        @sd_srv.stop unless @sd_srv.stopped?
        @sd_srv.before_shutdown unless @sd_srv.before_shutdown?
        @sd_srv.shutdown unless @sd_srv.shutdown?
        @sd_srv.after_shutdown unless @sd_srv.after_shutdown?
        @sd_srv.close unless @sd_srv.closed?
        @sd_srv.terminate unless @sd_srv.terminated?
      end
    end

    test 'Skip if srv record is not updated' do
      @sd_srv.extend(TestTimerEventHelperWrapper)
      mock(Resolv::DNS).new { flexmock('dns_resolver', getresources: [SRV_RECORD2, SRV_RECORD1], getaddress: '127.0.0.1') }
      @sd_srv.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com' }))
      queue = []

      @sd_srv.start(queue)
      assert_empty queue

      @sd_srv.resume
      assert_empty queue
    end

    test 'Skip if DNS resolver raise an error' do
      @sd_srv.extend(TestTimerEventHelperWrapper)
      mock = flexmock('dns_resolver', getaddress: '127.0.0.1')
               .should_receive(:getresources)
               .and_return([SRV_RECORD1, SRV_RECORD2])
               .and_return { raise 'some error' } # for start
               .and_return { raise 'some error' } # for resume
               .mock

      mock(Resolv::DNS).new { mock }
      @sd_srv.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com' }))
      queue = []

      @sd_srv.start(queue)
      assert_empty queue

      @sd_srv.resume
      assert_empty queue
    end

    test 'if service is updated, service_in and service_out event happen' do
      @sd_srv.extend(TestTimerEventHelperWrapper)
      mock = flexmock('dns_resolver', getaddress: '127.0.0.1')
               .should_receive(:getresources)
               .and_return([SRV_RECORD1])
               .and_return([SRV_RECORD2])
               .mock

      mock(Resolv::DNS).new { mock }
      @sd_srv.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com', 'dns_lookup' => false }))
      queue = []

      @sd_srv.start(queue)
      join = queue.shift
      drain = queue.shift
      assert_equal Fluent::Plugin::ServiceDiscovery::SERVICE_IN, join.type
      assert_equal 8082, join.service.port
      assert_equal 'service2.example.com', join.service.host

      assert_equal Fluent::Plugin::ServiceDiscovery::SERVICE_OUT, drain.type
      assert_equal 8081, drain.service.port
      assert_equal 'service1.example.com', drain.service.host
    end

    test 'if service is deleted, service_out event happens' do
      @sd_srv.extend(TestTimerEventHelperWrapper)
      mock = flexmock('dns_resolver', getaddress: '127.0.0.1')
               .should_receive(:getresources)
               .and_return([SRV_RECORD1, SRV_RECORD2])
               .and_return([SRV_RECORD2])
               .mock

      mock(Resolv::DNS).new { mock }
      @sd_srv.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com', 'dns_lookup' => false }))
      queue = []

      @sd_srv.start(queue)

      assert_equal 1, queue.size
      drain = queue.shift
      assert_equal Fluent::Plugin::ServiceDiscovery::SERVICE_OUT, drain.type
      assert_equal 8081, drain.service.port
      assert_equal 'service1.example.com', drain.service.host
    end

    test 'if new service is added, service_in event happens' do
      @sd_srv.extend(TestTimerEventHelperWrapper)
      mock = flexmock('dns_resolver', getaddress: '127.0.0.1')
               .should_receive(:getresources)
               .and_return([SRV_RECORD2])
               .and_return([SRV_RECORD1, SRV_RECORD2])
               .mock

      mock(Resolv::DNS).new { mock }
      @sd_srv.configure(config_element('service_discovery', '', { 'service' => 'service1', 'hostname' => 'example.com', 'dns_lookup' => false }))
      queue = []

      @sd_srv.start(queue)

      assert_equal 1, queue.size
      join = queue.shift
      assert_equal Fluent::Plugin::ServiceDiscovery::SERVICE_IN, join.type
      assert_equal 8081, join.service.port
      assert_equal 'service1.example.com', join.service.host
    end
  end
end
