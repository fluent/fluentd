#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'socket'
require 'ipaddr'
require 'openssl'

require_relative 'socket_option'

module Fluent
  module PluginHelper
    module Socket
      # stop     : [-]
      # shutdown : [-]
      # close    : [-]
      # terminate: [-]

      include Fluent::PluginHelper::SocketOption

      TLS_DEFAULT_VERSION = :'TLSv1_2'
      TLS_SUPPORTED_VERSIONS = [:'TLSv1_1', :'TLSv1_2']
      ### follow httpclient configuration by nahi
      # OpenSSL 0.9.8 default: "ALL:!ADH:!LOW:!EXP:!MD5:+SSLv2:@STRENGTH"
      CIPHERS_DEFAULT = "ALL:!aNULL:!eNULL:!SSLv2" # OpenSSL >1.0.0 default

      attr_reader :_sockets # for tests

      # TODO: implement connection pool for specified host

      def socket_create(proto, host, port, **kwargs, &block)
        case proto
        when :tcp
          socket_create_tcp(host, port, **kwargs, &block)
        when :udp
          socket_create_udp(host, port, **kwargs, &block)
        when :tls
          socket_create_tls(host, port, **kwargs, &block)
        when :unix
          raise "not implemented yet"
        else
          raise ArgumentError, "invalid protocol: #{proto}"
        end
      end

      def socket_create_tcp(host, port, resolve_name: false, **kwargs, &block)
        sock = WrappedSocket::TCP.new(host, port)
        socket_option_set(sock, resolve_name: resolve_name, **kwargs)
        if block
          begin
            block.call(sock)
          ensure
            sock.close_write rescue nil
            sock.close rescue nil
          end
        else
          sock
        end
      end

      def socket_create_udp(host, port, resolve_name: false, connect: false, **kwargs, &block)
        family = IPAddr.new(IPSocket.getaddress(host)).ipv4? ? ::Socket::AF_INET : ::Socket::AF_INET6
        sock = WrappedSocket::UDP.new(family)
        socket_option_set(sock, resolve_name: resolve_name, **kwargs)
        sock.connect(host, port) if connect
        if block
          begin
            block.call(sock)
          ensure
            sock.close rescue nil
          end
        else
          sock
        end
      end

      def socket_create_tls(
          host, port,
          version: TLS_DEFAULT_VERSION, ciphers: CIPHERS_DEFAULT, insecure: false, verify_fqdn: true, fqdn: nil,
          enable_system_cert_store: true, allow_self_signed_cert: false, cert_paths: nil,
          cert_path: nil, private_key_path: nil, private_key_passphrase: nil, **kwargs, &block)

        host_is_ipaddress = IPAddr.new(host) rescue false
        fqdn ||= host unless host_is_ipaddress

        context = OpenSSL::SSL::SSLContext.new(version)

        if insecure
          log.trace "setting TLS verify_mode NONE"
          context.verify_mode = OpenSSL::SSL::VERIFY_NONE
        else
          cert_store = OpenSSL::X509::Store.new
          if allow_self_signed_cert && OpenSSL::X509.const_defined?('V_FLAG_CHECK_SS_SIGNATURE')
            cert_store.flags = OpenSSL::X509::V_FLAG_CHECK_SS_SIGNATURE
          end
          begin
            if enable_system_cert_store
              log.trace "loading system default certificate store"
              cert_store.set_default_paths
            end
          rescue OpenSSL::X509::StoreError
            log.warn "failed to load system default certificate store", error: e
          end
          if cert_paths
            if cert_paths.respond_to?(:each)
              cert_paths.each do |cert_path|
                log.trace "adding CA cert", path: cert_path
                cert_store.add_file(cert_path)
              end
            else
              cert_path = cert_paths
              log.trace "adding CA cert", path: cert_path
              cert_store.add_file(cert_path)
            end
          end

          log.trace "setting TLS context", mode: "peer", ciphers: ciphers
          context.set_params({})
          context.ciphers = ciphers
          context.verify_mode = OpenSSL::SSL::VERIFY_PEER
          context.cert_store = cert_store
          context.verify_hostname = true if verify_fqdn && fqdn && context.respond_to?(:verify_hostname=)
          context.cert = OpenSSL::X509::Certificate.new(File.read(cert_path)) if cert_path
          context.key = OpenSSL::PKey::RSA.new(File.read(private_key_path), private_key_passphrase) if private_key_path
        end

        tcpsock = socket_create_tcp(host, port, **kwargs)
        sock = WrappedSocket::TLS.new(tcpsock, context)
        sock.sync_close = true
        sock.hostname = fqdn if verify_fqdn && fqdn && sock.respond_to?(:hostname=)

        log.trace "entering TLS handshake"
        sock.connect

        begin
          if verify_fqdn
            log.trace "checking peer's certificate", subject: sock.peer_cert.subject
            sock.post_connection_check(fqdn)
            verify = sock.verify_result
            if verify != OpenSSL::X509::V_OK
              err_name = Socket.tls_verify_result_name(verify)
              log.warn "BUG: failed to verify certification while connecting (but not raised, why?)", host: host, fqdn: fqdn, error: err_name
              raise RuntimeError, "BUG: failed to verify certification and to handle it correctly while connecting host #{host} as #{fqdn}"
            end
          end
        rescue OpenSSL::SSL::SSLError => e
          log.warn "failed to verify certification while connecting tls session", host: host, fqdn: fqdn, error: e
          raise
        end

        if block
          begin
            block.call(sock)
          ensure
            sock.close rescue nil
          end
        else
          sock
        end
      end

      def self.tls_verify_result_name(code)
        case code
        when OpenSSL::X509::V_OK then 'V_OK'
        when OpenSSL::X509::V_ERR_AKID_SKID_MISMATCH then 'V_ERR_AKID_SKID_MISMATCH'
        when OpenSSL::X509::V_ERR_APPLICATION_VERIFICATION then 'V_ERR_APPLICATION_VERIFICATION'
        when OpenSSL::X509::V_ERR_CERT_CHAIN_TOO_LONG then 'V_ERR_CERT_CHAIN_TOO_LONG'
        when OpenSSL::X509::V_ERR_CERT_HAS_EXPIRED then 'V_ERR_CERT_HAS_EXPIRED'
        when OpenSSL::X509::V_ERR_CERT_NOT_YET_VALID then 'V_ERR_CERT_NOT_YET_VALID'
        when OpenSSL::X509::V_ERR_CERT_REJECTED then 'V_ERR_CERT_REJECTED'
        when OpenSSL::X509::V_ERR_CERT_REVOKED then 'V_ERR_CERT_REVOKED'
        when OpenSSL::X509::V_ERR_CERT_SIGNATURE_FAILURE then 'V_ERR_CERT_SIGNATURE_FAILURE'
        when OpenSSL::X509::V_ERR_CERT_UNTRUSTED then 'V_ERR_CERT_UNTRUSTED'
        when OpenSSL::X509::V_ERR_CRL_HAS_EXPIRED then 'V_ERR_CRL_HAS_EXPIRED'
        when OpenSSL::X509::V_ERR_CRL_NOT_YET_VALID then 'V_ERR_CRL_NOT_YET_VALID'
        when OpenSSL::X509::V_ERR_CRL_SIGNATURE_FAILURE then 'V_ERR_CRL_SIGNATURE_FAILURE'
        when OpenSSL::X509::V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT then 'V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT'
        when OpenSSL::X509::V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD then 'V_ERR_ERROR_IN_CERT_NOT_AFTER_FIELD'
        when OpenSSL::X509::V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD then 'V_ERR_ERROR_IN_CERT_NOT_BEFORE_FIELD'
        when OpenSSL::X509::V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD then 'V_ERR_ERROR_IN_CRL_LAST_UPDATE_FIELD'
        when OpenSSL::X509::V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD then 'V_ERR_ERROR_IN_CRL_NEXT_UPDATE_FIELD'
        when OpenSSL::X509::V_ERR_INVALID_CA then 'V_ERR_INVALID_CA'
        when OpenSSL::X509::V_ERR_INVALID_PURPOSE then 'V_ERR_INVALID_PURPOSE'
        when OpenSSL::X509::V_ERR_KEYUSAGE_NO_CERTSIGN then 'V_ERR_KEYUSAGE_NO_CERTSIGN'
        when OpenSSL::X509::V_ERR_OUT_OF_MEM then 'V_ERR_OUT_OF_MEM'
        when OpenSSL::X509::V_ERR_PATH_LENGTH_EXCEEDED then 'V_ERR_PATH_LENGTH_EXCEEDED'
        when OpenSSL::X509::V_ERR_SELF_SIGNED_CERT_IN_CHAIN then 'V_ERR_SELF_SIGNED_CERT_IN_CHAIN'
        when OpenSSL::X509::V_ERR_SUBJECT_ISSUER_MISMATCH then 'V_ERR_SUBJECT_ISSUER_MISMATCH'
        when OpenSSL::X509::V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY then 'V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY'
        when OpenSSL::X509::V_ERR_UNABLE_TO_DECRYPT_CERT_SIGNATURE then 'V_ERR_UNABLE_TO_DECODE_ISSUER_PUBLIC_KEY'
        when OpenSSL::X509::V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE then 'V_ERR_UNABLE_TO_DECRYPT_CRL_SIGNATURE'
        when OpenSSL::X509::V_ERR_UNABLE_TO_GET_CRL then 'V_ERR_UNABLE_TO_GET_CRL'
        when OpenSSL::X509::V_ERR_UNABLE_TO_GET_ISSUER_CERT then 'V_ERR_UNABLE_TO_GET_ISSUER_CERT'
        when OpenSSL::X509::V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY then 'V_ERR_UNABLE_TO_GET_ISSUER_CERT_LOCALLY'
        when OpenSSL::X509::V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE then 'V_ERR_UNABLE_TO_VERIFY_LEAF_SIGNATURE'
        end
      end

      # socket_create_socks ?

      module WrappedSocket
        class TCP < ::TCPSocket
          def remote_addr; peeraddr[3]; end
          def remote_host; peeraddr[2]; end
          def remote_port; peeraddr[1]; end
        end
        class UDP < ::UDPSocket
          def remote_addr; peeraddr[3]; end
          def remote_host; peeraddr[2]; end
          def remote_port; peeraddr[1]; end
        end
        class TLS < OpenSSL::SSL::SSLSocket
          def remote_addr; peeraddr[3]; end
          def remote_host; peeraddr[2]; end
          def remote_port; peeraddr[1]; end
        end
      end

      def initialize
        super
        # @_sockets = [] # for keepalived sockets / connection pool
      end

      # def close
      #   @_sockets.each do |sock|
      #     sock.close
      #   end
      #   super
      # end
    end
  end
end
