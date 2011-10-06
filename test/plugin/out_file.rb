require 'fluent/test'
require 'fileutils'
require 'time'

class FileOutputTest < Test::Unit::TestCase
  def test_configure
    d = create_driver %[
      path testpath
      compress gz
    ]
    assert_equal 'testpath', d.instance.path
    assert_equal :gz, d.instance.compress
  end

  def test_format
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC")
    d.emit({"a"=>"b"}, time)
    d.emit({"a"=>"b"}, time)

    d.expect_format %[2011-01-02T13:14:15Z\ttest\t{"a":"b"}\n]
    d.expect_format %[2011-01-02T13:14:15Z\ttest\t{"a":"b"}\n]

    d.run
  end

  def test_write
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC")
    d.emit({"a"=>"b"}, time)
    d.emit({"a"=>"b"}, time)

    # FileOutput#write returns path
    path = d.run
    expect_path = "#{TMP_DIR}/out_file_test._0.log.gz"
    assert_equal expect_path, path

    data = Zlib::GzipReader.open(expect_path) {|f| f.read }
    assert_equal %[2011-01-02T13:14:15Z\ttest\t{"a":"b"}\n] +
                    %[2011-01-02T13:14:15Z\ttest\t{"a":"b"}\n],
                 data
  end

  def test_write_path_increment
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC")
    d.emit({"a"=>"b"}, time)
    d.emit({"a"=>"b"}, time)

    # FileOutput#write returns path
    path = d.run
    assert_equal "#{TMP_DIR}/out_file_test._0.log.gz", path

    path = d.run
    assert_equal "#{TMP_DIR}/out_file_test._1.log.gz", path

    path = d.run
    assert_equal "#{TMP_DIR}/out_file_test._2.log.gz", path
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp"

  DEFAULT_CONFIG = %[
    path #{TMP_DIR}/out_file_test
    compress gz
    utc
  ]

  def create_driver(conf = DEFAULT_CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::FileOutput).configure(conf)
  end

  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end
end

