require_relative 'helper'
require 'fluent/tls'

class UniqueIdTest < Test::Unit::TestCase
  TEST_TLS1_1_CASES = {
    'New TLS v1.1' => :'TLS1_1',
    'Old TLS v1.1' => :'TLSv1_1',
  }
  TEST_TLS1_2_CASES = {
    'New TLS v1.2' => :'TLS1_2',
    'Old TLS v1.2' => :'TLSv1_2'
  } 
  TEST_TLS_CASES = TEST_TLS1_1_CASES.merge(TEST_TLS1_2_CASES)

  sub_test_case 'constants' do
    test 'default version' do
      assert_equal :'TLSv1_2', Fluent::TLS::DEFAULT_VERSION
    end

    data(TEST_TLS_CASES)
    test 'supported versions' do |ver|
      assert_include Fluent::TLS::SUPPORTED_VERSIONS, ver
    end

    test 'default ciphers' do
      assert_equal "ALL:!aNULL:!eNULL:!SSLv2", Fluent::TLS::CIPHERS_DEFAULT
    end
  end

  sub_test_case 'set_version_to_context' do
    setup do
      @ctx = OpenSSL::SSL::SSLContext.new
    end

    data(TEST_TLS_CASES)
    test 'with version' do |ver|
      assert_nothing_raised {
        Fluent::TLS.set_version_to_context(@ctx, ver, nil, nil)
      }
    end

    data(TEST_TLS_CASES)
    test 'can specify old/new syntax to min_version/max_version' do |ver|
      omit "min_version=/max_version= is not supported" unless Fluent::TLS::MIN_MAX_AVAILABLE

      assert_nothing_raised {
        Fluent::TLS.set_version_to_context(@ctx, Fluent::TLS::DEFAULT_VERSION, ver, ver)
      }
    end

    test 'raise ConfigError when either one of min_version/max_version is not specified' do
      omit "min_version=/max_version= is not supported" unless Fluent::TLS::MIN_MAX_AVAILABLE

      ver = Fluent::TLS::DEFAULT_VERSION
      assert_raise(Fluent::ConfigError) {
        Fluent::TLS.set_version_to_context(@ctx, ver, ver, nil)
      }
      assert_raise(Fluent::ConfigError) {
        Fluent::TLS.set_version_to_context(@ctx, ver, nil, ver)
      }
    end
  end
end
