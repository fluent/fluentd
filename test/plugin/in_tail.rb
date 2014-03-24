require 'fluent/test'
require 'net/http'

class TailInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/tail#{ENV['TEST_ENV_NUMBER']}"

  COMMON_CONFIG = %[
    path #{TMP_DIR}/tail.txt
    tag t1
    rotate_wait 2s
    pos_file #{TMP_DIR}/tail.pos
  ]
  SINGLE_LINE_CONFIG = %[
    format /(?<message>.*)/
  ]

  def create_driver(conf = SINGLE_LINE_CONFIG, use_common_conf = true)
    config = use_common_conf ? COMMON_CONFIG + conf : conf
    Fluent::Test::InputTestDriver.new(Fluent::NewTailInput).configure(config)
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

  # multiline mode test

  def test_multiline
    File.open("#{TMP_DIR}/tail.txt", "w") { |f| }

    d = create_driver %[
      format multiline
      format1 /^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.*))?/
      format_firstline /^[s]/
    ]
    d.run do
      File.open("#{TMP_DIR}/tail.txt", "a") { |f|
        f.puts "f test1"
        f.puts "s test2"
        f.puts "f test3"
        f.puts "f test4"
        f.puts "s test5"
        f.puts "s test6"
        f.puts "f test7"
        f.puts "s test8"
      }
      sleep 1
    end

    emits = d.emits
    assert(emits.length > 0)
    assert_equal({"message1" => "test2", "message2" => "test3", "message3" => "test4"}, emits[0][2])
    assert_equal({"message1" => "test5"}, emits[1][2])
    assert_equal({"message1" => "test6", "message2" => "test7"}, emits[2][2])
    assert_equal({"message1" => "test8"}, emits[3][2])
  end
  
  def test_multiline_with_multiple_formats
    File.open("#{TMP_DIR}/tail.txt", "w") { |f| }

    d = create_driver %[
      format multiline
      format1 /^s (?<message1>[^\\n]+)\\n?/
      format2 /(f (?<message2>[^\\n]+)\\n?)?/
      format3 /(f (?<message3>.*))?/
      format_firstline /^[s]/
    ]
    d.run do
      File.open("#{TMP_DIR}/tail.txt", "a") { |f|
        f.puts "f test1"
        f.puts "s test2"
        f.puts "f test3"
        f.puts "f test4"
        f.puts "s test5"
        f.puts "s test6"
        f.puts "f test7"
        f.puts "s test8"
      }
      sleep 1
    end

    emits = d.emits
    assert(emits.length > 0)
    assert_equal({"message1" => "test2", "message2" => "test3", "message3" => "test4"}, emits[0][2])
    assert_equal({"message1" => "test5"}, emits[1][2])
    assert_equal({"message1" => "test6", "message2" => "test7"}, emits[2][2])
    assert_equal({"message1" => "test8"}, emits[3][2])
  end

  def test_multilinelog_with_multiple_paths
    files = ["#{TMP_DIR}/tail1.txt", "#{TMP_DIR}/tail2.txt"]
    files.each { |file| File.open(file, "w") { |f| } }

    d = create_driver(%[
      path #{files[0]},#{files[1]}
      tag t1
      format multiline
      format1 /^[s|f] (?<message>.*)/
      format_firstline /^[s]/
    ], false)
    d.run do
      files.each do |file|
        File.open(file, 'a') { |f|
          f.puts "f #{file} line should be ignored"
          f.puts "s test1"
          f.puts "f test2"
          f.puts "f test3"
          f.puts "s test4"
        }
      end
      sleep 1
    end

    emits = d.emits
    assert_equal({"message" => "test1\nf test2\nf test3"}, emits[0][2])
    assert_equal({"message" => "test1\nf test2\nf test3"}, emits[1][2])
    # "test4" events are here because these events are flushed at shutdown phase
    assert_equal({"message" => "test4"}, emits[2][2])
    assert_equal({"message" => "test4"}, emits[3][2])
  end

  def test_path_key
    File.open("#{TMP_DIR}/tail.txt", "w") { |f| }

    d = create_driver(%[
      path #{TMP_DIR}/tail.txt
      tag t1
      format /(?<message>.*)/
      path_key path
    ], false)

    d.run do
      sleep 1

      File.open("#{TMP_DIR}/tail.txt", "a") {|f|
        f.puts "test1"
        f.puts "test2"
      }
      sleep 1
    end

    emits = d.emits
    assert_equal(true, emits.length > 0)
    assert_equal({"message"=>"test1", "path"=>"#{TMP_DIR}/tail.txt"}, emits[0][2])
    assert_equal({"message"=>"test2", "path"=>"#{TMP_DIR}/tail.txt"}, emits[1][2])
  end

end
