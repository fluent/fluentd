require_relative '../helper'
require 'fluent/plugin_helper/server'
require 'fluent/plugin_helper/cert_option'

class CertOptionPluginHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :cert_option
  end

  class DummyServer < Fluent::Plugin::TestBase
    helpers :server
  end

  test 'can load PEM encoded certificate file' do
    d = Dummy.new
    certs = d.cert_option_certificates_from_file("test/plugin_helper/data/cert/cert.pem")
    assert_equal(1, certs.length)
    certs = d.cert_option_certificates_from_file("test/plugin_helper/data/cert/cert-with-no-newline.pem")
    assert_equal(1, certs.length)
    certs = d.cert_option_certificates_from_file("test/plugin_helper/data/cert/cert-with-CRLF.pem")
    assert_equal(1, certs.length)
  end

  test 'raise an error for broken certificates_from_file file' do
    d = Dummy.new
    assert_raise Fluent::ConfigError do
      d.cert_option_certificates_from_file("test/plugin_helper/data/cert/empty.pem")
    end
  end

  sub_test_case "ensure OpenSSL FIPS mode" do
    setup do
      cert_dir = File.expand_path(File.join(File.dirname(__FILE__), "../plugin_helper/data/cert/"))
      @tls_options = {
        cert_path: File.join(cert_dir, "cert.pem"),
        private_key_path: File.join(cert_dir, "cert-key.pem"),
      }
      @d = DummyServer.new
    end

    data(
      enabled_fips_mode: [true, true, nil],
      skip_checking_fips_mode: [true, false, nil],
      block_incompatible_fips_mode: [false, true,
                                     Fluent::ConfigError.new("Cannot enable FIPS compliant mode. OpenSSL FIPS configuration is disabled")],
      not_care_fips_mode: [false, false, nil]
    )
    test 'ensure FIPS error' do |(fips_mode, ensure_fips, expected)|
      stub(OpenSSL).fips_mode { fips_mode }
      conf = @d.server_create_transport_section_object(@tls_options.merge({ensure_fips: ensure_fips}))
      if expected
        assert_raise(expected) do
          @d.cert_option_create_context(Fluent::TLS::DEFAULT_VERSION,
                                        false,
                                        Fluent::TLS::CIPHERS_DEFAULT,
                                        conf)
        end
      else
        assert_nothing_raised do
          @d.cert_option_create_context(Fluent::TLS::DEFAULT_VERSION,
                                        false,
                                        Fluent::TLS::CIPHERS_DEFAULT,
                                        conf)
        end
      end
    end
  end
end
