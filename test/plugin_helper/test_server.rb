require_relative '../helper'
require 'fluent/plugin_helper/server'
require 'fluent/plugin_helper/cert_option' # to create certs for tests
require 'fluent/plugin/base'
require 'timeout'

require 'serverengine'
require 'fileutils'

class ServerPluginHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :server
  end

  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/plugin_helper_server")

  PORT = unused_port

  setup do
    @socket_manager_path = ServerEngine::SocketManager::Server.generate_path
    if @socket_manager_path.is_a?(String) && File.exist?(@socket_manager_path)
      FileUtils.rm_f @socket_manager_path
    end
    @socket_manager_server = ServerEngine::SocketManager::Server.open(@socket_manager_path)
    ENV['SERVERENGINE_SOCKETMANAGER_PATH'] = @socket_manager_path.to_s

    @d = Dummy.new
    @d.start
    @d.after_start
  end

  teardown do
    (@d.stopped? || @d.stop) rescue nil
    (@d.before_shutdown? || @d.before_shutdown) rescue nil
    (@d.shutdown? || @d.shutdown) rescue nil
    (@d.after_shutdown? || @d.after_shutdown) rescue nil
    (@d.closed? || @d.close) rescue nil
    (@d.terminated? || @d.terminate) rescue nil

    @socket_manager_server.close
    if @socket_manager_server.is_a?(String) && File.exist?(@socket_manager_path)
      FileUtils.rm_f @socket_manager_path
    end
  end

  # run tests for tcp, udp, tls and unix
  sub_test_case '#server_create and #server_create_connection' do
    methods = {server_create: :server_create, server_create_connection: :server_create_connection}

    data(
      'server_create tcp'  => [:server_create, :tcp, {}],
      'server_create udp'  => [:server_create, :udp, {max_bytes: 128}],
      'server_create tls'  => [:server_create, :tls, {tls_options: {insecure: true}}],
      # 'server_create unix'  = > [:server_create, :unix, {}],
      'server_create_connection tcp'  => [:server_create, :tcp, {}],
      'server_create_connection tls'  => [:server_create, :tls, {tls_options: {insecure: true}}],
      # 'server_create_connection unix'  = > [:server_create, :unix, {}],
    )
    test 'cannot create 2 or more servers using same bind address and port if shared option is false' do |(m, proto, kwargs)|
      begin
        d2 = Dummy.new; d2.start; d2.after_start

        assert_nothing_raised do
          @d.__send__(m, :myserver, PORT, bind: '127.0.0.1', proto: proto, shared: false, **kwargs){|x| x }
        end
        assert_raise(Errno::EADDRINUSE, Errno::EACCES) do
          d2.__send__(m, :myserver, PORT, bind: '127.0.0.1', proto: proto, **kwargs){|x| x }
        end
      ensure
        d2.stop; d2.before_shutdown; d2.shutdown; d2.after_shutdown; d2.close; d2.terminate
      end
    end
  end

  module CertUtil
    extend Fluent::PluginHelper::CertOption
  end

  def create_ca_options
    {
      private_key_length: 2048,
      country: 'US',
      state: 'CA',
      locality: 'Mountain View',
      common_name: 'ca.testing.fluentd.org',
      expiration: 30 * 86400,
      digest: :sha256,
    }
  end

  def create_server_options
    {
      private_key_length: 2048,
      country: 'US',
      state: 'CA',
      locality: 'Mountain View',
      common_name: 'server.testing.fluentd.org',
      expiration: 30 * 86400,
      digest: :sha256,
    }
  end

  def write_cert_and_key(cert_path, cert, key_path, key, passphrase)
    File.open(cert_path, "w"){|f| f.write(cert.to_pem) }
    # Write the secret key (raw or encrypted by AES256) in PEM format
    key_str = passphrase ? key.export(OpenSSL::Cipher.new("AES-256-CBC"), passphrase) : key.export
    File.open(key_path, "w"){|f| f.write(key_str) }
    File.chmod(0600, cert_path, key_path)
  end

  def create_server_pair_signed_by_self(cert_path, private_key_path, passphrase)
    cert, key, _ = CertUtil.cert_option_generate_server_pair_self_signed(create_server_options)
    write_cert_and_key(cert_path, cert, private_key_path, key, passphrase)
    return cert
  end

  def create_ca_pair_signed_by_self(cert_path, private_key_path, passphrase)
    cert, key, _ = CertUtil.cert_option_generate_ca_pair_self_signed(create_ca_options)
    write_cert_and_key(cert_path, cert, private_key_path, key, passphrase)
  end

  def create_server_pair_signed_by_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, passphrase)
    cert, key, _ = CertUtil.cert_option_generate_server_pair_by_ca(ca_cert_path, ca_key_path, ca_key_passphrase, create_server_options)
    write_cert_and_key(cert_path, cert, private_key_path, key, passphrase)
    return cert
  end

  def create_server_pair_chained_with_root_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, passphrase)
    root_cert, root_key, _ = CertUtil.cert_option_generate_ca_pair_self_signed(create_ca_options)
    write_cert_and_key(ca_cert_path, root_cert, ca_key_path, root_key, ca_key_passphrase)

    intermediate_ca_options = create_ca_options
    intermediate_ca_options[:common_name] = 'ca2.testing.fluentd.org'
    chain_cert, chain_key = CertUtil.cert_option_generate_pair(intermediate_ca_options, root_cert.subject)
    chain_cert.add_extension OpenSSL::X509::Extension.new('basicConstraints', OpenSSL::ASN1.Sequence([OpenSSL::ASN1::Boolean(true)]))
    chain_cert.sign(root_key, "sha256")

    server_cert, server_key, _ = CertUtil.cert_option_generate_pair(create_server_options, chain_cert.subject)
    server_cert.add_extension OpenSSL::X509::Extension.new('basicConstraints', OpenSSL::ASN1.Sequence([OpenSSL::ASN1::Boolean(false)]))
    server_cert.add_extension OpenSSL::X509::Extension.new('nsCertType', 'server')
    server_cert.sign(chain_key, "sha256")

    # write chained cert
    File.open(cert_path, "w") do |f|
      f.write server_cert.to_pem
      f.write chain_cert.to_pem
    end
    key_str = passphrase ? server_key.export(OpenSSL::Cipher.new("AES-256-CBC"), passphrase) : server_key.export
    File.open(private_key_path, "w"){|f| f.write(key_str) }
    File.chmod(0600, cert_path, private_key_path)
  end

  def open_tls_session(addr, port, verify: true, cert_path: nil, selfsigned: true, hostname: nil)
    context = OpenSSL::SSL::SSLContext.new
    context.set_params({})
    if verify
      cert_store = OpenSSL::X509::Store.new
      cert_store.set_default_paths
      if selfsigned && OpenSSL::X509.const_defined?('V_FLAG_CHECK_SS_SIGNATURE')
        cert_store.flags = OpenSSL::X509::V_FLAG_CHECK_SS_SIGNATURE
      end
      if cert_path
        cert_store.add_file(cert_path)
      end
      context.verify_mode = OpenSSL::SSL::VERIFY_PEER
      context.cert_store = cert_store
      if !hostname && context.respond_to?(:verify_hostname=)
        context.verify_hostname = false # In test code, using hostname to be connected is very difficult
      end
    else
      context.verify_mode = OpenSSL::SSL::VERIFY_NONE
    end

    sock = OpenSSL::SSL::SSLSocket.new(TCPSocket.new(addr, port), context)
    sock.hostname = hostname if hostname && sock.respond_to?(:hostname)
    sock.connect
    yield sock
  ensure
    sock.close rescue nil
  end

  def assert_certificate(cert, expected_extensions)
    get_extension = lambda do |oid|
      cert.extensions.detect { |e| e.oid == oid }
    end

    assert_true cert.serial > 1
    assert_equal 2, cert.version

    expected_extensions.each do |ext|
      expected_oid, expected_value = ext
      assert_equal expected_value, get_extension.call(expected_oid).value
    end
  end

  def open_client(proto, addr, port)
    client = case proto
             when :tcp
               TCPSocket.open(addr, port)
             when :tls
               c = OpenSSL::SSL::SSLSocket.new(TCPSocket.open(addr, port))
               c.sync_close = true
               c.connect
             else
               raise ArgumentError, "unknown proto:#{proto}"
             end
    yield client
  ensure
    client.close rescue nil
  end
end
