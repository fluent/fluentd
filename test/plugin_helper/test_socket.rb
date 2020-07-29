require_relative '../helper'
require 'fluent/plugin_helper/socket'
require 'fluent/plugin/base'

require 'socket'
require 'openssl'

class SocketHelperTest < Test::Unit::TestCase
  PORT = unused_port
  CERT_DIR = File.expand_path(File.dirname(__FILE__) + '/data/cert/without_ca')
  CA_CERT_DIR = File.expand_path(File.dirname(__FILE__) + '/data/cert/with_ca')
  CERT_CHAINS_DIR = File.expand_path(File.dirname(__FILE__) + '/data/cert/cert_chains')

  class SocketHelperTestPlugin < Fluent::Plugin::TestBase
    helpers :socket
  end

  class EchoTLSServer
    def initialize(host = '127.0.0.1', port = PORT, cert_path: nil, private_key_path: nil, ca_path: nil)
      server = TCPServer.open(host, port)
      ctx = OpenSSL::SSL::SSLContext.new
      ctx.cert = OpenSSL::X509::Certificate.new(File.open(cert_path)) if cert_path

      cert_store = OpenSSL::X509::Store.new
      cert_store.set_default_paths
      cert_store.add_file(ca_path) if ca_path
      ctx.cert_store = cert_store

      ctx.key = OpenSSL::PKey::RSA.new(File.open(private_key_path)) if private_key_path
      ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER
      ctx.verify_hostname = false

      @server = OpenSSL::SSL::SSLServer.new(server, ctx)
      @thread = nil
      @r, @w = IO.pipe
    end

    def start
      do_start

      if block_given?
        begin
          yield
          @thread.join(5)
        ensure
          stop
        end
      end
    end

    def stop
      unless @w.closed?
        @w.write('stop')
      end

      [@server, @w, @r].each do |s|
        next if s.closed?
        s.close
      end

      @thread.join(5)
    end

    private

    def do_start
      @thread = Thread.new(@server) do |s|
        socks, _, _ = IO.select([s.accept, @r], nil, nil)

        if socks.include?(@r)
          break
        end

        sock = socks.first
        buf = +''
        loop do
          b = sock.read_nonblock(1024, nil, exception: false)
          if b == :wait_readable || b.nil?
            break
          end
          buf << b
        end

        sock.write(buf)
        sock.close
      end
    end
  end

  test 'with self-signed cert/key pair' do
    cert_path = File.join(CERT_DIR, 'cert.pem')
    private_key_path = File.join(CERT_DIR, 'cert-key.pem')

    EchoTLSServer.new(cert_path: cert_path, private_key_path: private_key_path).start do
      client = SocketHelperTestPlugin.new.socket_create_tls('127.0.0.1', PORT, verify_fqdn: false, cert_paths: [cert_path])
      client.write('hello')
      assert_equal 'hello', client.readpartial(100)
      client.close
    end
  end

  test 'with cert/key signed by self-signed CA' do
    cert_path = File.join(CA_CERT_DIR, 'cert.pem')
    private_key_path = File.join(CA_CERT_DIR, 'cert-key.pem')

    ca_cert_path = File.join(CA_CERT_DIR, 'ca-cert.pem')

    EchoTLSServer.new(cert_path: cert_path, private_key_path: private_key_path).start do
      client = SocketHelperTestPlugin.new.socket_create_tls('127.0.0.1', PORT, verify_fqdn: false, cert_paths: [ca_cert_path])
      client.write('hello')
      assert_equal 'hello', client.readpartial(100)
      client.close
    end
  end

  test 'with cert/key signed by self-signed CA in server and client cert chain' do
    cert_path = File.join(CERT_DIR, 'cert.pem')
    private_key_path = File.join(CERT_DIR, 'cert-key.pem')

    client_ca_cert_path = File.join(CERT_CHAINS_DIR, 'ca-cert.pem')
    client_cert_path = File.join(CERT_CHAINS_DIR, 'cert.pem')
    client_private_key_path = File.join(CERT_CHAINS_DIR, 'cert-key.pem')

    EchoTLSServer.new(cert_path: cert_path, private_key_path: private_key_path, ca_path: client_ca_cert_path).start do
      client = SocketHelperTestPlugin.new.socket_create_tls('127.0.0.1', PORT, verify_fqdn: false, cert_path: client_cert_path, private_key_path: client_private_key_path, cert_paths: [cert_path])
      client.write('hello')
      assert_equal 'hello', client.readpartial(100)
      client.close
    end
  end

  test 'with empty cert file' do
    cert_path = File.expand_path(File.dirname(__FILE__) + '/data/cert/empty.pem')

    assert_raise Fluent::ConfigError do
      SocketHelperTestPlugin.new.socket_create_tls('127.0.0.1', PORT, cert_path: cert_path)
    end
  end
end
