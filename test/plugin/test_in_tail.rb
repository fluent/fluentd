require_relative '../helper'
require 'fluent/test/driver/input'
require 'fluent/plugin/in_tail'
require 'fluent/plugin/buffer'
require 'fluent/system_config'
require 'net/http'
require 'flexmock/test_unit'
require 'timecop'

class TailInputTest < Test::Unit::TestCase
  include FlexMock::TestCase

  def setup
    Fluent::Test.setup
    FileUtils.mkdir_p(TMP_DIR)
    system('ls', '-la', TMP_DIR) if File.exist?(TMP_DIR)
  end

  def teardown
    super
    Fluent::Engine.stop
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR, secure: true)
    if File.exist?(TMP_DIR)
      # ensure files are closed for Windows, on which deleted files
      # are still visible from filesystem
      GC.start(full_mark: true, immediate_mark: true, immediate_sweep: true)
      FileUtils.remove_entry_secure(TMP_DIR, true)
    end
    system('ls', '-la', TMP_DIR) if File.exist?(TMP_DIR)
  end

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/tail#{ENV['TEST_ENV_NUMBER']}"

  CONFIG = config_element("ROOT", "", {
                            "path" => "#{TMP_DIR}/tail.txt",
                            "tag" => "t1",
                            "rotate_wait" => "2s"
                          })
  COMMON_CONFIG = CONFIG + config_element("", "", { "pos_file" => "#{TMP_DIR}/tail.pos" })
  CONFIG_READ_FROM_HEAD = config_element("", "", { "read_from_head" => true })
  CONFIG_ENABLE_WATCH_TIMER = config_element("", "", { "enable_watch_timer" => false })
  SINGLE_LINE_CONFIG = config_element("", "", { "format" => "/(?<message>.*)/" })
  PARSE_SINGLE_LINE_CONFIG = config_element("", "", {}, [config_element("parse", "", { "@type" => "/(?<message>.*)/" })])
  MULTILINE_CONFIG = config_element(
    "", "", {
      "format" => "multiline",
      "format1" => "/^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.*))?/",
      "format_firstline" => "/^[s]/"
    })
  PARSE_MULTILINE_CONFIG = config_element(
    "", "", {},
    [config_element("parse", "", {
                      "@type" => "multiline",
                      "format1" => "/^s (?<message1>[^\\n]+)(\\nf (?<message2>[^\\n]+))?(\\nf (?<message3>.*))?/",
                      "format_firstline" => "/^[s]/"
                    })
    ])

  def create_driver(conf = SINGLE_LINE_CONFIG, use_common_conf = true)
    config = use_common_conf ? COMMON_CONFIG + conf : conf
    Fluent::Test::Driver::Input.new(Fluent::Plugin::TailInput).configure(config)
  end

  def test_lf
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f| }

    d = create_driver

    d.run(expect_emits: 1) do
      File.open("#{TMP_DIR}/tail.txt", "wb") {|f|
        f.puts "test4"
      }
    end
  end

  def test_whitespace
    File.open("#{TMP_DIR}/tail.txt", "wb") {|f| }
  end
end
