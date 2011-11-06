require 'fluent/test'
require 'net/http'

class ExecInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    command bash -c "echo -e '2011-01-02 13:14:15\ttag1\tok'"
    keys time,tag,k1
    time_key time
    tag_key tag
    time_format %Y-%m-%d %H:%M:%S
    run_interval 1s
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::ExecInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal ["time","tag","k1"], d.instance.keys
    assert_equal "tag", d.instance.tag_key
    assert_equal "time", d.instance.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.time_format
  end

  def test_emit
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15").to_i

    d.run do
      sleep 2
    end

    emits = d.emits
    assert_equal true, emits.length > 0
    assert_equal ["tag1", time, {"k1"=>"ok"}], emits[0]
  end
end

