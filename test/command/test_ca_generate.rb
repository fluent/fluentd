require_relative '../helper'

require 'yajl'
require 'flexmock/test_unit'
require 'tmpdir'

require 'fluent/command/ca_generate'
require 'fluent/event'

class TestFluentCaGenerate < ::Test::Unit::TestCase
  def test_generate_ca_pair
    cert, key = Fluent::CaGenerate.generate_ca_pair(Fluent::CaGenerate::DEFAULT_OPTIONS)
    assert_equal(OpenSSL::X509::Certificate, cert.class)
    assert_true(key.private?)
  end

  def test_ca_generate
    dumped_output = capture_stdout do
      Dir.mktmpdir do |dir|
        Fluent::CaGenerate.new([dir, "fluentd"]).call
        assert_true(File.exist?(File.join(dir, "ca_key.pem")))
        assert_true(File.exist?(File.join(dir, "ca_cert.pem")))
      end
    end
    expected = <<TEXT
successfully generated: ca_key.pem, ca_cert.pem
copy and use ca_cert.pem to client(out_forward)
TEXT
    assert_equal(expected, dumped_output)
  end

  sub_test_case "configure options" do
    test "should respond multiple options" do
      dumped_output = capture_stdout do
        Dir.mktmpdir do |dir|
          Fluent::CaGenerate.new([dir, "fluentd",
                                  "--country", "JP", "--key-length", "4096",
                                  "--state", "Tokyo", "--locality", "Chiyoda-ku",
                                  "--common-name", "Forward CA"]).call
          assert_true(File.exist?(File.join(dir, "ca_key.pem")))
          assert_true(File.exist?(File.join(dir, "ca_cert.pem")))
        end
      end
      expected = <<TEXT
successfully generated: ca_key.pem, ca_cert.pem
copy and use ca_cert.pem to client(out_forward)
TEXT
      assert_equal(expected, dumped_output)
    end

    test "invalid options" do
      Dir.mktmpdir do |dir|
        assert_raise(OptionParser::InvalidOption) do
          Fluent::CaGenerate.new([dir, "fluentd",
                                  "--invalid"]).call
        end
        assert_false(File.exist?(File.join(dir, "ca_key.pem")))
        assert_false(File.exist?(File.join(dir, "ca_cert.pem")))
      end
    end

    test "empty options" do
      assert_raise(SystemExit) do
        capture_stdout do
          Fluent::CaGenerate.new([]).call
        end
      end
    end
  end
end
