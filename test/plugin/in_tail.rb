require 'fluent/test'
require 'net/http'

class TailInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp"

  CONFIG = %[
    path #{TMP_DIR}/tail.txt
    tag t1
    rotate_wait 2s
    pos_file #{TMP_DIR}/tail.pos
    format /(?<message>.*)/
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::TailInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal ["#{TMP_DIR}/tail.txt"], d.instance.paths
    assert_equal "t1", d.instance.tag
    assert_equal 2, d.instance.rotate_wait
    assert_equal "#{TMP_DIR}/tail.pos", d.instance.pos_file
  end

  def test_emit
    File.open("#{TMP_DIR}/tail.txt", "w") {|f|
      f.puts "test1"
      f.puts "test2"
    }

    d = create_driver

    d.run do
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "a") {|f|
        f.puts "test3"
        f.puts "test4"
      }
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message"=>"test3"}, emits[0][2])
    assert_equal({"message"=>"test4"}, emits[1][2])
  end

  def test_lf
    File.open("#{TMP_DIR}/tail.txt", "w") {|f| }

    d = create_driver

    d.run do
      File.open("#{TMP_DIR}/tail.txt", "a") {|f|
        f.print "test3"
      }
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "a") {|f|
        f.puts "test4"
      }
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message"=>"test3test4"}, emits[0][2])
  end

  def test_whitespace
    File.open("#{TMP_DIR}/tail.txt", "w") {|f| }

    d = create_driver

    d.run do
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "a") {|f|
        f.puts "    "		# 4 spaces
        f.puts "    4 spaces"
        f.puts "4 spaces    "
        f.puts "	"	# tab
        f.puts "	tab"
        f.puts "tab	"
      }
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message"=>"    "}, emits[0][2])
    assert_equal({"message"=>"    4 spaces"}, emits[1][2])
    assert_equal({"message"=>"4 spaces    "}, emits[2][2])
    assert_equal({"message"=>"	"}, emits[3][2])
    assert_equal({"message"=>"	tab"}, emits[4][2])
    assert_equal({"message"=>"tab	"}, emits[5][2])
  end
end
