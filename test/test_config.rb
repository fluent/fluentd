require_relative 'helper'
require 'fluent/config'
require 'fluent/config/parser'
require 'fluent/supervisor'
require 'fluent/load'
require 'fileutils'

class ConfigTest < Test::Unit::TestCase
  include Fluent

  TMP_DIR = File.dirname(__FILE__) + "/tmp/config#{ENV['TEST_ENV_NUMBER']}"

  def read_config(path)
    path = File.expand_path(path)
    File.open(path) { |io|
      Fluent::Config::Parser.parse(io, File.basename(path), File.dirname(path))
    }
  end

  def prepare_config
    write_config "#{TMP_DIR}/config_test_1.conf", %[
      k1 root_config
      include dir/config_test_2.conf  #
      include #{TMP_DIR}/config_test_4.conf
      include file://#{TMP_DIR}/config_test_5.conf
      <include config.d/*.conf />
    ]
    write_config "#{TMP_DIR}/dir/config_test_2.conf", %[
      k2 relative_path_include
      include ../config_test_3.conf
    ]
    write_config "#{TMP_DIR}/config_test_3.conf", %[
      k3 relative_include_in_included_file
    ]
    write_config "#{TMP_DIR}/config_test_4.conf", %[
      k4 absolute_path_include
    ]
    write_config "#{TMP_DIR}/config_test_5.conf", %[
      k5 uri_include
    ]
    write_config "#{TMP_DIR}/config.d/config_test_6.conf", %[
      k6 wildcard_include_1
      <elem1 name>
        include normal_parameter
      </elem1>
    ]
    write_config "#{TMP_DIR}/config.d/config_test_7.conf", %[
      k7 wildcard_include_2
    ]
    write_config "#{TMP_DIR}/config.d/config_test_8.conf", %[
      <elem2 name>
        <include ../dir/config_test_9.conf />
      </elem2>
    ]
    write_config "#{TMP_DIR}/dir/config_test_9.conf", %[
      k9 embedded
      <elem3 name>
        nested nested_value
        include hoge
      </elem3>
    ]
    write_config "#{TMP_DIR}/config.d/00_config_test_8.conf", %[
      k8 wildcard_include_3
      <elem4 name>
        include normal_parameter
      </elem4>
    ]

  end

  def test_include
    prepare_config
    c = read_config("#{TMP_DIR}/config_test_1.conf")
    assert_equal 'root_config', c['k1']
    assert_equal 'relative_path_include', c['k2']
    assert_equal 'relative_include_in_included_file', c['k3']
    assert_equal 'absolute_path_include', c['k4']
    assert_equal 'uri_include', c['k5']
    assert_equal 'wildcard_include_1', c['k6']
    assert_equal 'wildcard_include_2', c['k7']
    assert_equal 'wildcard_include_3', c['k8']
    assert_equal [
      'k1',
      'k2',
      'k3',
      'k4',
      'k5',
      'k8', # Because of the file name this comes first.
      'k6',
      'k7',
    ], c.keys

    elem1 = c.elements.find { |e| e.name == 'elem1' }
    assert_not_nil elem1
    assert_equal 'name', elem1.arg
    assert_equal 'normal_parameter', elem1['include']

    elem2 = c.elements.find { |e| e.name == 'elem2' }
    assert_not_nil elem2
    assert_equal 'name', elem2.arg
    assert_equal 'embedded', elem2['k9']
    assert !elem2.has_key?('include')

    elem3 = elem2.elements.find { |e| e.name == 'elem3' }
    assert_not_nil elem3
    assert_equal 'nested_value', elem3['nested']
    assert_equal 'hoge', elem3['include']
  end

  def test_check_not_fetchd
    write_config "#{TMP_DIR}/config_test_not_fetched.conf", %[
      <match dummy>
       type          rewrite
       add_prefix    filtered
       <rule>
         key     path
         pattern ^[A-Z]+
         replace
       </rule>
     </match>
    ]
    root_conf  = read_config("#{TMP_DIR}/config_test_not_fetched.conf")
    match_conf = root_conf.elements.first
    rule_conf  = match_conf.elements.first

    not_fetched = []; root_conf.check_not_fetched {|key, e| not_fetched << key }
    assert_equal %w[type add_prefix key pattern replace], not_fetched

    not_fetched = []; match_conf.check_not_fetched {|key, e| not_fetched << key }
    assert_equal %w[type add_prefix key pattern replace], not_fetched

    not_fetched = []; rule_conf.check_not_fetched {|key, e| not_fetched << key }
    assert_equal %w[key pattern replace], not_fetched

    # accessing should delete
    match_conf['type']
    rule_conf['key']

    not_fetched = []; root_conf.check_not_fetched {|key, e| not_fetched << key }
    assert_equal %w[add_prefix pattern replace], not_fetched

    not_fetched = []; match_conf.check_not_fetched {|key, e| not_fetched << key }
    assert_equal %w[add_prefix pattern replace], not_fetched

    not_fetched = []; rule_conf.check_not_fetched {|key, e| not_fetched << key }
    assert_equal %w[pattern replace], not_fetched

    # repeatedly accessing should not grow memory usage
    before_size = match_conf.unused.size
    10.times { match_conf['type'] }
    assert_equal before_size, match_conf.unused.size
  end

  def write_config(path, data)
    FileUtils.mkdir_p(File.dirname(path))
    File.open(path, "w") {|f| f.write data }
  end

  def test_inline
    prepare_config
    opts = {
        :config_path => "#{TMP_DIR}/config_test_1.conf",
        :inline_config => "<source>\n  type http\n  port 2222\n </source>"
    }
    assert_nothing_raised do
      Fluent::Supervisor.new(opts)
    end
    create_warn_dummy_logger
  end

  def create_warn_dummy_logger
    dl_opts = {}
    dl_opts[:log_level] = ServerEngine::DaemonLogger::WARN
    logdev = Fluent::Test::DummyLogDevice.new
    logger = ServerEngine::DaemonLogger.new(logdev, dl_opts)
    $log = Fluent::Log.new(logger)
  end
end

