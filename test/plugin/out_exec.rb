require 'fluent/test'
require 'fileutils'

class ExecOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp"

  CONFIG = %[
    buffer_path #{TMP_DIR}/buffer
    command cat >#{TMP_DIR}/out
    keys time,tag,k1
    tag_key tag
    time_key time
    time_format %Y-%m-%d %H:%M:%S
    localtime
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::ExecOutput).configure(conf)
  end

  def test_configure
    d = create_driver

    assert_equal ["time","tag","k1"], d.instance.keys
    assert_equal "tag", d.instance.tag_key
    assert_equal "time", d.instance.time_key
    assert_equal "%Y-%m-%d %H:%M:%S", d.instance.time_format
    assert_equal true, d.instance.localtime
  end

  def test_format
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15").to_i
    d.emit({"k1"=>"v1","kx"=>"vx"}, time)
    d.emit({"k1"=>"v2","kx"=>"vx"}, time)

    d.expect_format %[2011-01-02 13:14:15\ttest\tv1\n]
    d.expect_format %[2011-01-02 13:14:15\ttest\tv2\n]

    d.run
  end

  def test_write
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15").to_i
    d.emit({"k1"=>"v1","kx"=>"vx"}, time)
    d.emit({"k1"=>"v2","kx"=>"vx"}, time)

    d.run

    expect_path = "#{TMP_DIR}/out"
    assert_equal true, File.exist?(expect_path)

    data = File.read(expect_path)
    expect_data =
      %[2011-01-02 13:14:15\ttest\tv1\n] +
      %[2011-01-02 13:14:15\ttest\tv2\n]
    assert_equal expect_data, data
  end
end

