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

require 'openssl'
require 'socket'

# this module is only for Socket/Server plugin helpers
module Fluent
  module PluginHelper
    module CertOption
      def cert_option_create_context(version, insecure, ciphers, conf)
        cert, key, extra = cert_option_server_validate!(conf)

        ctx = OpenSSL::SSL::SSLContext.new(version)
        unless insecure
          # inject OpenSSL::SSL::SSLContext::DEFAULT_PARAMS
          # https://bugs.ruby-lang.org/issues/9424
          ctx.set_params({})

          ctx.ciphers = ciphers
        end

        if conf.client_cert_auth
            ctx.verify_mode = OpenSSL::SSL::VERIFY_PEER | OpenSSL::SSL::VERIFY_FAIL_IF_NO_PEER_CERT
        end

        ctx.ca_file = conf.ca_path
        ctx.cert = cert
        ctx.key = key
        if extra && !extra.empty?
          ctx.extra_chain_cert = extra
        end

        ctx
      end

      def cert_option_server_validate!(conf)
        case
        when conf.cert_path
          raise Fluent::ConfigError, "private_key_path is required when cert_path is specified" unless conf.private_key_path
          log.warn "For security reason, setting private_key_passphrase is recommended when cert_path is specified" unless conf.private_key_passphrase
          cert_option_load(conf.cert_path, conf.private_key_path, conf.private_key_passphrase)

        when conf.ca_cert_path
          raise Fluent::ConfigError, "ca_private_key_path is required when ca_cert_path is specified" unless conf.ca_private_key_path
          log.warn "For security reason, setting ca_private_key_passphrase is recommended when ca_cert_path is specified" unless conf.ca_private_key_passphrase
          generate_opts = cert_option_cert_generation_opts_from_conf(conf)
          cert_option_generate_server_pair_by_ca(
            conf.ca_cert_path,
            conf.ca_private_key_path,
            conf.ca_private_key_passphrase,
            generate_opts
          )

        when conf.insecure
          log.warn "insecure TLS communication server is configured (using 'insecure' mode)"
          generate_opts = cert_option_cert_generation_opts_from_conf(conf)
          cert_option_generate_server_pair_self_signed(generate_opts)

        else
          raise Fluent::ConfigError, "no valid cert options configured. specify either 'cert_path', 'ca_cert_path' or 'insecure'"
        end
      end

      def cert_option_load(cert_path, private_key_path, private_key_passphrase)
        key = OpenSSL::PKey::RSA.new(File.read(private_key_path), private_key_passphrase)
        certs = cert_option_certificates_from_file(cert_path)
        cert = certs.shift
        return cert, key, certs
      end

      def cert_option_cert_generation_opts_from_conf(conf)
        {
          private_key_length: conf.generate_private_key_length,
          country: conf.generate_cert_country,
          state: conf.generate_cert_state,
          locality: conf.generate_cert_locality,
          common_name: conf.generate_cert_common_name || ::Socket.gethostname,
          expiration: conf.generate_cert_expiration,
          digest: conf.generate_cert_digest,
        }
      end

      def cert_option_generate_pair(opts, issuer = nil)
        key = OpenSSL::PKey::RSA.generate(opts[:private_key_length])

        subject = OpenSSL::X509::Name.new
        subject.add_entry('C', opts[:country])
        subject.add_entry('ST', opts[:state])
        subject.add_entry('L', opts[:locality])
        subject.add_entry('CN', opts[:common_name])

        issuer ||= subject

        cert = OpenSSL::X509::Certificate.new
        cert.not_before = Time.at(0)
        cert.not_after = Time.now + opts[:expiration]
        cert.public_key = key
        cert.version = 2
        cert.serial = rand(2**(8*10))
        cert.issuer = issuer
        cert.subject  = subject

        return cert, key
      end

      def cert_option_add_extensions(cert, extensions)
        ef = OpenSSL::X509::ExtensionFactory.new
        extensions.each do |ext|
          oid, value = ext
          cert.add_extension ef.create_extension(oid, value)
        end
      end

      def cert_option_generate_ca_pair_self_signed(generate_opts)
        cert, key = cert_option_generate_pair(generate_opts)

        cert_option_add_extensions(cert, [
          ['basicConstraints', 'CA:TRUE']
        ])

        cert.sign(key, generate_opts[:digest].to_s)
        return cert, key
      end

      def cert_option_generate_server_pair_by_ca(ca_cert_path, ca_key_path, ca_key_passphrase, generate_opts)
        ca_key = OpenSSL::PKey::RSA.new(File.read(ca_key_path), ca_key_passphrase)
        ca_cert = OpenSSL::X509::Certificate.new(File.read(ca_cert_path))
        cert, key = cert_option_generate_pair(generate_opts, ca_cert.subject)
        raise "BUG: certificate digest algorithm not set" unless generate_opts[:digest]

        cert_option_add_extensions(cert, [
          ['basicConstraints', 'CA:FALSE'],
          ['nsCertType', 'server'],
          ['keyUsage', 'digitalSignature,keyEncipherment'],
          ['extendedKeyUsage', 'serverAuth']
        ])

        cert.sign(ca_key, generate_opts[:digest].to_s)
        return cert, key, nil
      end

      def cert_option_generate_server_pair_self_signed(generate_opts)
        cert, key = cert_option_generate_pair(generate_opts)
        raise "BUG: certificate digest algorithm not set" unless generate_opts[:digest]

        cert_option_add_extensions(cert, [
          ['basicConstraints', 'CA:FALSE'],
          ['nsCertType', 'server']
        ])

        cert.sign(key, generate_opts[:digest].to_s)
        return cert, key, nil
      end

      def cert_option_certificates_from_file(path)
        data = File.read(path)
        pattern = Regexp.compile('-+BEGIN CERTIFICATE-+\n(?:[^-]*\n)+-+END CERTIFICATE-+\n', Regexp::MULTILINE)
        list = []
        data.scan(pattern){|match| list << OpenSSL::X509::Certificate.new(match) }
        list
      end
    end
  end
end
