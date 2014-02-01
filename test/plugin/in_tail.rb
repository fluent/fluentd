require 'fluent/test'
require 'net/http'

class TailInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/tail#{ENV['TEST_ENV_NUMBER']}"

  CONFIG = %[
    path #{TMP_DIR}/tail.txt
    tag t1
    rotate_wait 2s
    pos_file #{TMP_DIR}/tail.pos
    format /(?<message>.*)/
  ]

  CONFIG_WITHOUT_POS_FILE = %[
    path #{TMP_DIR}/tail.txt
    tag t1
    rotate_wait 2s
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

  def test_emit_without_pos_file
    File.open("#{TMP_DIR}/tail.txt", "w") {|f|
      f.puts "test1"
      f.puts "test2"
    }

    d = create_driver(CONFIG_WITHOUT_POS_FILE)

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

  def test_rotate_file
    sub_test_rotate_file(CONFIG)
  end

  def test_rotate_file_without_pos_file
    sub_test_rotate_file(CONFIG_WITHOUT_POS_FILE)
  end

  def test_rotate_file_when_fluent_doesnot_start
    sub_test_rotate_file_when_fluent_doesnot_start(CONFIG)
  end

  def sub_test_rotate_file(config)
    File.open("#{TMP_DIR}/tail.txt", "w") {|f|
      f.puts "test1"
      f.puts "test2"
    }

    d = create_driver(config)

    d.run do
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "a") {|f|
        f.puts "test3"
        f.puts "test4"
      }
      sleep 1

      FileUtils.mv("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail2.txt")
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "w") {|f| }
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "a") {|f|
        f.puts "test5"
        f.puts "test6"
      }
      sleep 1
    end

    d.run do
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message"=>"test3"}, emits[0][2])
    assert_equal({"message"=>"test4"}, emits[1][2])
    assert_equal({"message"=>"test5"}, emits[2][2])
    assert_equal({"message"=>"test6"}, emits[3][2])
  end

  # A log file rotates when fluentd does not start.
  def sub_test_rotate_file_when_fluent_doesnot_start(config)
    File.open("#{TMP_DIR}/tail.txt", "w") {|f|
      f.puts "test1"
      f.puts "test2"
    }

    d = create_driver(config)

    d.run do
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "a") {|f|
        f.puts "test3"
        f.puts "test4"
      }
      sleep 1

      FileUtils.mv("#{TMP_DIR}/tail.txt", "#{TMP_DIR}/tail2.txt")
    end

    File.open("#{TMP_DIR}/tail.txt", "w") {|f| }
    sleep 1

    File.open("#{TMP_DIR}/tail.txt", "a") {|f|
      f.puts "test5"
      f.puts "test6"
    }
    sleep 1

    d.run do
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message"=>"test3"}, emits[0][2])
    assert_equal({"message"=>"test4"}, emits[1][2])
    assert_equal({"message"=>"test5"}, emits[2][2])
    assert_equal({"message"=>"test6"}, emits[3][2])
  end
end
