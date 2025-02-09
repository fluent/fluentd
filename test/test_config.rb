require_relative 'helper'
require 'fluent/config'
require 'fluent/config/parser'
require 'fluent/supervisor'
require 'fluent/load'
require 'fileutils'

class ConfigTest < Test::Unit::TestCase
  include Fluent

  TMP_DIR = File.dirname(__FILE__) + "/tmp/config#{ENV['TEST_ENV_NUMBER']}"

  def read_config(path, use_yaml: false)
    path = File.expand_path(path)
    if use_yaml
      context = Kernel.binding

      s = Fluent::Config::YamlParser::Loader.new(context).load(Pathname.new(path))
      Fluent::Config::YamlParser::Parser.new(s).build.to_element
    else
      File.open(path) { |io|
        Fluent::Config::Parser.parse(io, File.basename(path), File.dirname(path))
      }
    end
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

  sub_test_case "yaml config" do
    def test_included
      write_config "#{TMP_DIR}/config_test_not_fetched.yaml", <<-EOS
      system:
        umask: "0022" 
      config:
        - source:
            $type: dummy
            tag: tag.dummy
        - source:
            $type: tcp
            $log_level: info
            tag: tag.tcp
            parse:
              $arg:
                - why.parse.section.doesnot.have.arg
                - huh
              $type: none
        - match:
            $tag: tag.*
            $type: stdout
            $log_level: debug
            buffer:
              $type: memory
              flush_interval: 1s
        - !include fluent-included.yaml
      EOS
      write_config "#{TMP_DIR}/fluent-included.yaml", <<-EOS
      - label:
          $name: '@FLUENT_LOG'
          config:
            - match:
                $type: "null"
                $tag: "**"
                buffer:
                  $type: memory
                  flush_mode: interval
                  flush_interval: 1s
      EOS
      root_conf = read_config("#{TMP_DIR}/config_test_not_fetched.yaml", use_yaml: true)
      system_conf = root_conf.elements.find { |e| e.name == "system" }
      dummy_source_conf = root_conf.elements.find { |e| e.name == "source" && e['@type'] == "dummy" }
      tcp_source_conf = root_conf.elements.find { |e| e.name == "source" && e['@type'] == "tcp" }
      parse_tcp_conf = tcp_source_conf.elements.first
      match_conf = root_conf.elements.find { |e| e.name == "match" && e.arg == "tag.*" }
      label_conf = root_conf.elements.find { |e| e.name == "label" }
      fluent_log_conf = label_conf.elements.first
      fluent_log_buffer_conf = fluent_log_conf.elements.first

      assert_equal(
        [
          'dummy',
          'tag.dummy',
          'tcp',
          'tag.tcp',
          'info',
          'none',
          'why.parse.section.doesnot.have.arg,huh',
          'stdout',
          'tag.*',
          'debug',
          'null',
          '**',
          '@FLUENT_LOG',
          'memory',
          'interval',
          '1s',
          '0022',
        ],
        [
          dummy_source_conf['@type'],
          dummy_source_conf['tag'],
          tcp_source_conf['@type'],
          tcp_source_conf['tag'],
          tcp_source_conf['@log_level'],
          parse_tcp_conf['@type'],
          parse_tcp_conf.arg,
          match_conf['@type'],
          match_conf.arg,
          match_conf['@log_level'],
          fluent_log_conf['@type'],
          fluent_log_conf.arg,
          label_conf.arg,
          fluent_log_buffer_conf['@type'],
          fluent_log_buffer_conf['flush_mode'],
          fluent_log_buffer_conf['flush_interval'],
          system_conf['umask'],
        ])
    end

    def test_included_glob
      write_config "#{TMP_DIR}/config.yaml", <<-EOS
      system:
        umask: "0022"
      config:
        - !include "include/*.yaml"
      EOS
      write_config "#{TMP_DIR}/include/02_source2.yaml", <<-EOS
      - source:
          $type: dummy
          tag: tag.dummy
      EOS
      write_config "#{TMP_DIR}/include/01_source1.yaml", <<-EOS
      - source:
          $type: tcp
          tag: tag.tcp
          parse:
            $arg:
              - why.parse.section.doesnot.have.arg
              - huh
            $type: none
      EOS
      write_config "#{TMP_DIR}/include/03_match1.yaml", <<-EOS
      - match:
          $tag: tag.*
          $type: stdout
          buffer:
            $type: memory
            flush_interval: 1s
      EOS
      root_conf = read_config("#{TMP_DIR}/config.yaml", use_yaml: true)
      system_conf = root_conf.elements.find { |e| e.name == "system" }
      tcp_source_conf = root_conf.elements.find { |e| e.name == "source" && e['@type'] == "tcp" }
      dummy_source_conf = root_conf.elements.find { |e| e.name == "source" && e['@type'] == "dummy" }
      parse_tcp_conf = tcp_source_conf.elements.first
      match_conf = root_conf.elements.find { |e| e.name == "match" && e.arg == "tag.*" }

      assert_equal(
        [
          'tcp',
          'tag.tcp',
          'none',
          'why.parse.section.doesnot.have.arg,huh',
          'dummy',
          'tag.dummy',
          'stdout',
          'tag.*',
          '0022',
        ],
        [
          tcp_source_conf['@type'],
          tcp_source_conf['tag'],
          parse_tcp_conf['@type'],
          parse_tcp_conf.arg,
          dummy_source_conf['@type'],
          dummy_source_conf['tag'],
          match_conf['@type'],
          match_conf.arg,
          system_conf['umask'],
        ])
    end

    def test_check_not_fetchd
      write_config "#{TMP_DIR}/config_test_not_fetched.yaml", <<-EOS
      config:
        - match:
            $arg: dummy
            $type: rewrite
            add_prefix:    filtered
            rule:
              key:     path
              pattern: "^[A-Z]+"
              replace: true
      EOS
      root_conf  = read_config("#{TMP_DIR}/config_test_not_fetched.yaml", use_yaml: true)
      match_conf = root_conf.elements.first
      rule_conf  = match_conf.elements.first

      not_fetched = []; root_conf.check_not_fetched {|key, e| not_fetched << key }
      assert_equal %w[@type $arg add_prefix key pattern replace], not_fetched

      not_fetched = []; match_conf.check_not_fetched {|key, e| not_fetched << key }
      assert_equal %w[@type $arg add_prefix key pattern replace], not_fetched

      not_fetched = []; rule_conf.check_not_fetched {|key, e| not_fetched << key }
      assert_equal %w[key pattern replace], not_fetched

      # accessing should delete
      match_conf['type']
      rule_conf['key']

      not_fetched = []; root_conf.check_not_fetched {|key, e| not_fetched << key }
      assert_equal %w[@type $arg add_prefix pattern replace], not_fetched

      not_fetched = []; match_conf.check_not_fetched {|key, e| not_fetched << key }
      assert_equal %w[@type $arg add_prefix pattern replace], not_fetched

      not_fetched = []; rule_conf.check_not_fetched {|key, e| not_fetched << key }
      assert_equal %w[pattern replace], not_fetched

      # repeatedly accessing should not grow memory usage
      before_size = match_conf.unused.size
      10.times { match_conf['type'] }
      assert_equal before_size, match_conf.unused.size
    end

    test 'yaml system config with umask' do
      write_config "#{TMP_DIR}/config_test_umask.yaml", <<-EOS
      system:
        umask: "0022"
      config:
        - source:
            $type: dummy
            tag: tag.dummy
        - match:
            $tag: tag.*
            $type: stdout
      EOS
      conf = read_config("#{TMP_DIR}/config_test_umask.yaml", use_yaml: true)
      assert_equal "0022", conf.elements.find { |e| e.name == "system" }["umask"]
    end

    test 'system config with invalid umask' do
      conf = <<-EOC
      <system>
        umask 0999  # invalid octal
      </system>
      EOC
      conf = Fluent::Config.parse(conf, "(test)", "(test_dir)", true)
      assert_equal "0999", conf.elements.find { |e| e.name == "system" }["umask"]
    end

    test 'system config without umask' do
      conf = <<-EOC
      <system>
        # no umask setting
      </system>
      EOC
      conf = Fluent::Config.parse(conf, "(test)", "(test_dir)", true)
      assert_nil conf.elements.find { |e| e.name == "system" }["umask"]
    end
  end

  def write_config(path, data, encoding: 'utf-8')
    FileUtils.mkdir_p(File.dirname(path))
    File.open(path, "w:#{encoding}:utf-8") {|f| f.write data }
  end

  sub_test_case '.build' do
    test 'read config' do
      write_config("#{TMP_DIR}/build/config_build.conf", 'key value')
      c = Fluent::Config.build(config_path: "#{TMP_DIR}/build/config_build.conf")
      assert_equal('value', c['key'])
    end

    test 'read config with encoding' do
      write_config("#{TMP_DIR}/build/config_build2.conf", "#てすと\nkey value", encoding: 'shift_jis')

      c = Fluent::Config.build(config_path: "#{TMP_DIR}/build/config_build2.conf", encoding: 'shift_jis')
      assert_equal('value', c['key'])
    end

    test 'read config with additional_config' do
      write_config("#{TMP_DIR}/build/config_build2.conf", "key value")

      c = Fluent::Config.build(config_path: "#{TMP_DIR}/build/config_build2.conf", additional_config: 'key2 value2')
      assert_equal('value', c['key'])
      assert_equal('value2', c['key2'])
    end

    test 'system config with umask' do
      conf = <<-EOC
      <system>
        umask 0022
      </system>
      EOC
      conf = Fluent::Config.parse(conf, "(test)", "(test_dir)", true)
      ra = Fluent::RootAgent.new(log: $log)
      old_umask = File.umask
      begin
        ra.configure(conf)
        assert_equal 0022, File.umask
      ensure
        File.umask(old_umask)
      end
    end

    test 'system config with invalid umask' do
      conf = <<-EOC
      <system>
        umask 0999  # invalid octal
      </system>
      EOC
      conf = Fluent::Config.parse(conf, "(test)", "(test_dir)", true)
      ra = Fluent::RootAgent.new(log: $log)
      assert_raise(Fluent::ConfigError) do
        ra.configure(conf)
      end
    end

    test 'system config without umask' do
      conf = <<-EOC
      <system>
        # no umask setting
      </system>
      EOC
      conf = Fluent::Config.parse(conf, "(test)", "(test_dir)", true)
      ra = Fluent::RootAgent.new(log: $log)
      old_umask = File.umask
      begin
        ra.configure(conf)
        assert_equal 0022, File.umask
      ensure
        File.umask(old_umask)
      end
    end
  end
end