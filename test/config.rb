require File.dirname(__FILE__) + '/helper'
require 'fluent/config'
require 'fluent/supervisor'
require 'fluent/load'
require 'fileutils'

class ConfigTest < Test::Unit::TestCase
  include Fluent

  TMP_DIR = File.dirname(__FILE__) + "/tmp/config#{ENV['TEST_ENV_NUMBER']}"

  def prepare_config
    write_config "#{TMP_DIR}/config_test_1.conf", %[
      k1 root_config
      include dir/config_test_2.conf  #
      include #{TMP_DIR}/config_test_4.conf
      include file://#{TMP_DIR}/config_test_5.conf
      include config.d/*.conf
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
        embed ../dir/config_test_9.conf
      </elem2>
    ]
    write_config "#{TMP_DIR}/dir/config_test_9.conf", %[
      k9 embeded
    ]

  end

  def test_include
    prepare_config
    c = Config.read("#{TMP_DIR}/config_test_1.conf")
    assert_equal 'root_config', c['k1']
    assert_equal 'relative_path_include', c['k2']
    assert_equal 'relative_include_in_included_file', c['k3']
    assert_equal 'absolute_path_include', c['k4']
    assert_equal 'uri_include', c['k5']
    assert_equal 'wildcard_include_1', c['k6']
    assert_equal 'wildcard_include_2', c['k7']

    elem1 = c.elements.find { |e| e.name == 'elem1' }
    assert_not_nil elem1
    assert_equal   'name',             elem1.arg
    assert_equal   'normal_parameter', elem1['include']

    elem2 = c.elements.find { |e| e.name == 'elem2' }
    assert_not_nil elem2
    assert_equal   'name',    elem2.arg
    assert_equal   'embeded', elem2['k9']
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
  end
end

