require_relative '../helper'

require 'yajl'
require 'flexmock/test_unit'
require 'tmpdir'

require 'fluent/command/ca_generate'
require 'fluent/event'

class TestFluentCaGenerate < ::Test::Unit::TestCase
  def test_generate_ca_pair
    opt = {
      private_key_length: 2048,
      cert_country:  'US',
      cert_state:    'CA',
      cert_locality: 'Mountain View',
      cert_common_name: 'Fluentd Forward CA',
    }
    cert, key = Fluent::CaGenerate.generate_ca_pair(opt)
    assert_equal(OpenSSL::X509::Certificate, cert.class)
    assert_true(key.private?)
  end
end
