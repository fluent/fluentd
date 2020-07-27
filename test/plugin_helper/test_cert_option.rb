require_relative '../helper'
require 'fluent/plugin_helper/cert_option'

class CertOptionPluginHelperTest < Test::Unit::TestCase
  class Dummy < Fluent::Plugin::TestBase
    helpers :cert_option
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
      certs = d.cert_option_certificates_from_file("test/plugin_helper/data/cert/empty.pem")
    end
  end
end
