require 'helper'
require 'fluent/config/yaml_parser'
require 'socket'
require 'json'
require 'date'

class YamlParserTest < Test::Unit::TestCase
  TMP_DIR = File.dirname(__FILE__) + "/tmp/yaml_config#{ENV['TEST_ENV_NUMBER']}"

  def write_config(path, data, encoding: 'utf-8')
    FileUtils.mkdir_p(File.dirname(path))
    File.open(path, "w:#{encoding}:utf-8") {|f| f.write data }
  end

  sub_test_case 'Special YAML elements' do
    def test_special_yaml_elements_dollar
      write_config "#{TMP_DIR}/test_special_yaml_elements_dollar_source.yaml", <<~EOS
        config:
          - source:
              $type: dummy_type
              $label: dummy_label
              $id: dummy_id
              $log_level: debug
              $unknown: unknown
      EOS
      config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_special_yaml_elements_dollar_source.yaml")
      assert_equal('source', config.elements[0].name)
      assert_equal('dummy_type', config.elements[0]['@type'])
      assert_equal('dummy_label', config.elements[0]['@label'])
      assert_equal('dummy_id', config.elements[0]['@id'])
      assert_equal('debug', config.elements[0]['@log_level'])
      assert_nil(config.elements[0]['@unknown'])

      write_config "#{TMP_DIR}/test_special_yaml_elements_dollar_match.yaml", <<~EOS
        config:
          - match:
              $tag: dummy_tag
      EOS
      config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_special_yaml_elements_dollar_match.yaml")
      assert_equal('match', config.elements[0].name)
      assert_equal('dummy_tag', config.elements[0].arg)


      write_config "#{TMP_DIR}/test_special_yaml_elements_dollar_worker.yaml", <<~EOS
        config:
          - worker:
              $arg: dummy_arg
              config:
                - source:
                    $type: dummy
      EOS
      config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_special_yaml_elements_dollar_worker.yaml")
      assert_equal('worker', config.elements[0].name)
      assert_equal('dummy_arg', config.elements[0].arg)
    end

    def test_embedded_ruby_code
      write_config "#{TMP_DIR}/test_embedded_ruby_code.yaml", <<~EOS
        config:
          - source:
              host: !fluent/s "#{Socket.gethostname}"
      EOS
      config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_embedded_ruby_code.yaml")
      assert_equal(Socket.gethostname, config.elements[0]['host'])
    end

    def test_fluent_json_format
      write_config "#{TMP_DIR}/test_fluent_json_format.yaml", <<~EOS
        config:
          - source:
              hash_param: !fluent/json {
                "k": "v",
                "k1": 10
              }
      EOS
      config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_fluent_json_format.yaml")
      assert_equal({'k': 'v', 'k1': 10}.to_json, config.elements[0]['hash_param'])
    end
  end

  sub_test_case 'root elements' do
    def test_root_elements
      write_config "#{TMP_DIR}/test_root_elements.yaml", <<~EOS
        config:
          - source:
              $type: dummy
        system:
          dummy: dummy
        unknown:
          dummy: dummy
      EOS
      config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_root_elements.yaml")
      assert_equal(2, config.elements.size)
      # the first element is system section.
      assert_equal("system", config.elements[0].name)
      # the second element is source in config section.
      assert_equal("source", config.elements[1].name)
    end

    def test_root_elements_only_config_section
      write_config "#{TMP_DIR}/test_root_elements_only_config_section.yaml", <<~EOS
        config:
          - source:
              $type: dummy
      EOS
      config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_root_elements_only_config_section.yaml")
      assert_equal(1, config.elements.size)
      assert_equal("source", config.elements[0].name)
    end

    def test_root_elements_only_system_section
      write_config "#{TMP_DIR}/test_root_elements_only_system_section.yaml", <<~EOS
        system:
          dummy: dummy
      EOS
      config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_root_elements_only_system_section.yaml")
      assert_equal(1, config.elements.size)
      assert_equal("system", config.elements[0].name)
    end
  end

  sub_test_case 'config section' do
    def test_config_section_directives
      write_config "#{TMP_DIR}/dummy.yaml", <<~EOS
        - filter:
            $type: dummy
      EOS
      write_config "#{TMP_DIR}/test_config_section_directives.yaml", <<~EOS
        config:
          - source:
              $type: dummy
          - filter:
              $type: dummy
          - match:
              $tag: dummy
          - worker:
              $arg: 0
              config:
                - source:
                    $type: dummy
          - label:
              $name: dummy
              config:
                - filter:
                    $type: dummy
          - !include dummy.yaml
      EOS
      config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_config_section_directives.yaml")
      assert_equal(6, config.elements.size)
      assert_equal(%w(source filter match worker label filter), config.elements.map(&:name))
    end

    def test_config_section_unknown_directives
      write_config "#{TMP_DIR}/test_config_section_unknown_directives.yaml", <<~EOS
        config:
          - source:
              $type: dummy
          - unknown:
              $type: dummy
      EOS

      # TODO: it should raise Fluent::ConfigError instead of NoMethodError, or drop unknown directives
      assert_raise(NoMethodError) do
        Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_config_section_unknown_directives.yaml")
      end
    end

    sub_test_case 'label' do
      def test_label_section
        write_config "#{TMP_DIR}/test_label_section.yaml", <<~EOS
          config:
            - label:
                $name: dummy_label
                config:
                  - filter:
                      $type: dummy
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_label_section.yaml")
        assert_equal('label', config.elements[0].name)
        assert_equal('dummy_label', config.elements[0].arg)
        assert_equal('filter', config.elements[0].elements[0].name)
      end

      def test_label_section_missing_name
        write_config "#{TMP_DIR}/test_label_section_missing_name.yaml", <<~EOS
          config:
            - label:
                config:
                  - filter:
                      $type: dummy
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_label_section_missing_name.yaml")
        assert_equal('label', config.elements[0].name)
        assert_equal('', config.elements[0].arg)
        assert_equal('filter', config.elements[0].elements[0].name)
      end

      def test_label_section_missing_config
        write_config "#{TMP_DIR}/test_label_section_missing_config.yaml", <<~EOS
          config:
            - label:
                $name: dummy_label
        EOS

        # TODO: it should raise Fluent::ConfigError instead of NoMethodError
        assert_raise(NoMethodError) do
          Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_label_section_missing_config.yaml")
        end
      end
    end

    sub_test_case 'worker' do
      def test_worker_section
        write_config "#{TMP_DIR}/test_worker_section.yaml", <<~EOS
          config:
            - worker:
                $arg: 0
                config:
                  - source:
                      $type: dummy
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_worker_section.yaml")
        assert_equal('worker', config.elements[0].name)
        assert_equal('0', config.elements[0].arg)
        assert_equal('source', config.elements[0].elements[0].name)
      end

      def test_worker_section_missing_arg
        write_config "#{TMP_DIR}/test_worker_section_missing_arg.yaml", <<~EOS
          config:
            - worker:
                config:
                  - source:
                      $type: dummy
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_worker_section_missing_arg.yaml")
        assert_equal('worker', config.elements[0].name)
        assert_equal('', config.elements[0].arg)
        assert_equal('source', config.elements[0].elements[0].name)
      end

      def test_worker_section_missing_config
        write_config "#{TMP_DIR}/test_worker_section_missing_config.yaml", <<~EOS
          config:
            - worker:
                $arg: 0
        EOS

        # TODO: it should raise Fluent::ConfigError instead of NoMethodError
        assert_raise(NoMethodError) do
          Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_worker_section_missing_config.yaml")
        end
      end
    end

    sub_test_case 'source' do
      def test_source_section
        write_config "#{TMP_DIR}/test_source_section.yaml", <<~EOS
          config:
            - source:
                $type: dummy_type
                port: 8888
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_source_section.yaml")
        assert_equal('source', config.elements[0].name)
        assert_equal('dummy_type', config.elements[0]['@type'])
        assert_equal(8888, config.elements[0]['port'])
      end

      def test_source_section_missing_type
        write_config "#{TMP_DIR}/test_source_section_missing_type.yaml", <<~EOS
          config:
            - source:
                port: 8888
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_source_section_missing_type.yaml")
        assert_equal('source', config.elements[0].name)
        assert_equal(8888, config.elements[0]['port'])
        assert_nil(config.elements[0]['@type'])
      end
    end

    sub_test_case 'filter' do
      def test_filter_section
        write_config "#{TMP_DIR}/test_filter_section.yaml", <<~EOS
          config:
            - filter:
                $tag: dummy_tag
                $type: dummy_type
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_filter_section.yaml")
        assert_equal('filter', config.elements[0].name)
        assert_equal('dummy_tag', config.elements[0].arg)
        assert_equal('dummy_type', config.elements[0]['@type'])
      end

      def test_filter_section_multiple_tags
        write_config "#{TMP_DIR}/test_filter_section_multiple_tags_1.yaml", <<~EOS
          config:
            - filter:
                $tag: ['dummy_tag_A', 'dummy_tag_B']
                $type: dummy_type
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_filter_section_multiple_tags_1.yaml")
        assert_equal('filter', config.elements[0].name)
        assert_equal('dummy_tag_A,dummy_tag_B', config.elements[0].arg)
        assert_equal('dummy_type', config.elements[0]['@type'])

        write_config "#{TMP_DIR}/test_filter_section_multiple_tags_2.yaml", <<~EOS
          config:
            - filter:
                $tag:
                  - dummy_tag_C
                  - dummy_tag_D
                $type: dummy_type
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_filter_section_multiple_tags_2.yaml")
        assert_equal('filter', config.elements[0].name)
        assert_equal('dummy_tag_C,dummy_tag_D', config.elements[0].arg)
        assert_equal('dummy_type', config.elements[0]['@type'])
      end

      def test_filter_section_missing_tag
        write_config "#{TMP_DIR}/test_filter_section_missing_tag.yaml", <<~EOS
          config:
            - filter:
                $type: dummy_type
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_filter_section_missing_tag.yaml")
        assert_equal('filter', config.elements[0].name)
        assert_equal('', config.elements[0].arg)
        assert_equal('dummy_type', config.elements[0]['@type'])
      end

      def test_filter_section_missing_type
        write_config "#{TMP_DIR}/test_filter_section_missing_tag.yaml", <<~EOS
          config:
            - filter:
                $tag: dummy_tag
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_filter_section_missing_tag.yaml")
        assert_equal('filter', config.elements[0].name)
        assert_equal('dummy_tag', config.elements[0].arg)
        assert_nil(config.elements[0]['@type'])
      end
    end

    sub_test_case 'match' do
      def test_match_section
        write_config "#{TMP_DIR}/test_match_section.yaml", <<~EOS
          config:
            - match:
                $tag: dummy_tag
                $type: dummy_type
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_match_section.yaml")
        assert_equal('match', config.elements[0].name)
        assert_equal('dummy_tag', config.elements[0].arg)
        assert_equal('dummy_type', config.elements[0]['@type'])
      end

      def test_match_section_multiple_tags
        write_config "#{TMP_DIR}/test_match_section_multiple_tags_1.yaml", <<~EOS
          config:
            - match:
                $tag: ['dummy_tag_A', 'dummy_tag_B']
                $type: dummy_type
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_match_section_multiple_tags_1.yaml")
        assert_equal('match', config.elements[0].name)
        assert_equal('dummy_tag_A,dummy_tag_B', config.elements[0].arg)
        assert_equal('dummy_type', config.elements[0]['@type'])

        write_config "#{TMP_DIR}/test_match_section_multiple_tags_2.yaml", <<~EOS
          config:
            - match:
                $tag:
                  - dummy_tag_C
                  - dummy_tag_D
                $type: dummy_type
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_match_section_multiple_tags_2.yaml")
        assert_equal('match', config.elements[0].name)
        assert_equal('dummy_tag_C,dummy_tag_D', config.elements[0].arg)
        assert_equal('dummy_type', config.elements[0]['@type'])
      end

      def test_match_section_missing_tag
        write_config "#{TMP_DIR}/test_match_section_missing_tag.yaml", <<~EOS
          config:
            - match:
                $type: dummy_type
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_match_section_missing_tag.yaml")
        assert_equal('match', config.elements[0].name)
        assert_equal('', config.elements[0].arg)
        assert_equal('dummy_type', config.elements[0]['@type'])
      end

      def test_match_section_missing_type
        write_config "#{TMP_DIR}/test_match_section_missing_type.yaml", <<~EOS
          config:
            - match:
                $tag: dummy_tag
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_match_section_missing_type.yaml")
        assert_equal('match', config.elements[0].name)
        assert_equal('dummy_tag', config.elements[0].arg)
        assert_nil(config.elements[0]['@type'])
      end
    end

    sub_test_case '!include' do
      def test_include
        write_config "#{TMP_DIR}/dummy.yaml", <<~EOS
        - filter:
            $type: dummy
        EOS
        write_config "#{TMP_DIR}/test_include.yaml", <<~EOS
        config:
          - !include dummy.yaml
        EOS
        config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_include.yaml")
        assert_equal(1, config.elements.size)
        assert_equal("filter", config.elements[0].name) # included section
      end

      def test_include_normal_config_file
        write_config "#{TMP_DIR}/dummy.conf", <<~EOS
          <filter **>
            type dummy
          </filter>
        EOS
        write_config "#{TMP_DIR}/test_include_normal_config_file.yaml", <<~EOS
          config:
            - !include dummy.conf
        EOS

        # TODO: it should raise Fluent::ConfigError instead of TypeError, or parse normal config file
        assert_raise(TypeError) do
          Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_include_normal_config_file.yaml")
        end
      end
    end
  end

  def test_merge_common_parameter
    write_config "#{TMP_DIR}/test_merge_common_parameter.yaml", <<~EOS
      common_parameter: &common_parameter
        common_param: foobarbaz
      
      config:
        - match:
            $tag: dummy_tag_1
            $type: dummy_type_1
            <<: *common_parameter
        - match:
            $tag: dummy_tag_2
            $type: dummy_type_2
            <<: *common_parameter
    EOS
    config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_merge_common_parameter.yaml")
    assert_equal(2, config.elements.size)
    assert_equal('foobarbaz', config.elements[0]['common_param'])
    assert_equal('foobarbaz', config.elements[1]['common_param'])
  end

  def test_override_merged_common_parameter
    write_config "#{TMP_DIR}/test_merge_common_parameter.yaml", <<~EOS
      common_parameter: &common_parameter
        common_param1: foobarbaz
        common_param2: 12345
      
      config:
        - match:
            $tag: dummy_tag_1
            $type: dummy_type_1
            <<: *common_parameter
        - match:
            $tag: dummy_tag_2
            $type: dummy_type_2
            <<: *common_parameter
            common_param: override
    EOS
    config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_merge_common_parameter.yaml")
    assert_equal(2, config.elements.size)
    assert_equal('foobarbaz', config.elements[0]['common_param1'])
    assert_equal(12345, config.elements[0]['common_param2'])
    assert_equal('override', config.elements[1]['common_param'])
    assert_equal(12345, config.elements[1]['common_param2'])
  end

  def test_merge_common_parameter_using_include
    write_config "#{TMP_DIR}/fluent-common.yaml", <<~EOS
      - common_param: foobarbaz
    EOS

    write_config "#{TMP_DIR}/test_merge_common_parameter_using_include.yaml", <<~EOS
      config:
        - match:
            $tag: dummy_tag_1
            $type: dummy_type_1
            <<: !include fluent-common.yaml
        - match:
            $tag: dummy_tag_2
            $type: dummy_type_2
            <<: !include fluent-common.yaml
    EOS
    config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_merge_common_parameter_using_include.yaml")
    assert_equal(2, config.elements.size)
    assert_equal('foobarbaz', config.elements[0].elements[0]['common_param'])
    assert_equal('foobarbaz', config.elements[1].elements[0]['common_param'])
  end

  def test_unknown_anchor
    write_config "#{TMP_DIR}/test_unknown_anchor.yaml", <<~EOS
      common_parameter: &common_parameter
        common_param: foobarbaz
      
      config:
        - match:
            $tag: dummy_tag_1
            $type: dummy_type_1
            <<: *unknown_anchor
    EOS

    # TODO: it should raise Fluent::ConfigError instead of Psych::AnchorNotDefined
    assert_raises(Psych::AnchorNotDefined) do
      Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_unknown_anchor.yaml")
    end
  end

  def test_yaml_values
    write_config "#{TMP_DIR}/test_yaml_values.yaml", <<~EOS
      config:
        - source:
            mode: 0644
            float_value: 3.40
            flag_on: On
            flag_off: Off
            null: null
            date: 2026-01-01
            timestamp: 2026-01-01 00:00:00 +09:00
            symbol1: :foo
            symbol2: !ruby/symbol bar
            regexp: !ruby/regexp /^$/
    EOS
    config = Fluent::Config::YamlParser.parse("#{TMP_DIR}/test_yaml_values.yaml")
    assert_equal(420, config.elements[0]['mode'])
    assert_equal(3.4, config.elements[0]['float_value'])
    assert_equal(true, config.elements[0]['flag_on'])
    assert_equal(false, config.elements[0]['flag_off'])
    assert_nil(config.elements[0]['null'])
    assert_equal(Date.new(2026, 1, 1), config.elements[0]['date'])
    assert_equal(Time.parse("2026-01-01 00:00:00 +09:00"), config.elements[0]['timestamp'])
    assert_equal(:foo, config.elements[0]['symbol1'])
    assert_equal(:bar, config.elements[0]['symbol2'])
    assert_equal(/^$/, config.elements[0]['regexp'])
  end
end
