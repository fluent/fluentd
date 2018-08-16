require_relative '../helper'
require_relative "assertions"
require "json"
require "fluent/config/error"
require "fluent/config/basic_parser"
require "fluent/config/literal_parser"
require "fluent/config/v1_parser"
require 'fluent/config/parser'

module Fluent::Config
  module V1TestHelper
    def root(*elements)
      if elements.first.is_a?(Fluent::Config::Element)
        attrs = {}
      else
        attrs = elements.shift || {}
      end
      Fluent::Config::Element.new('ROOT', '', attrs, elements)
    end

    def e(name, arg='', attrs={}, elements=[])
      Fluent::Config::Element.new(name, arg, attrs, elements)
    end
  end

  class AllTypes
    include Fluent::Configurable

    config_param :param_string, :string
    config_param :param_enum, :enum, list: [:foo, :bar, :baz]
    config_param :param_integer, :integer
    config_param :param_float, :float
    config_param :param_size, :size
    config_param :param_bool, :bool
    config_param :param_time, :time
    config_param :param_hash, :hash
    config_param :param_array, :array
    config_param :param_regexp, :regexp
  end

  class TestV1Parser < ::Test::Unit::TestCase
    def read_config(path)
      path = File.expand_path(path)
      data = File.read(path)
      Fluent::Config::V1Parser.parse(data, File.basename(path), File.dirname(path))
    end

    def parse_text(text)
      basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
      Fluent::Config::V1Parser.parse(text, '(test)', basepath, nil)
    end

    include V1TestHelper
    extend V1TestHelper

    sub_test_case 'attribute parsing' do
      test "parses attributes" do
        assert_text_parsed_as(e('ROOT', '', {"k1"=>"v1", "k2"=>"v2"}), %[
          k1 v1
          k2 v2
        ])
      end

      test "allows attribute without value" do
        assert_text_parsed_as(e('ROOT', '', {"k1"=>"", "k2"=>"v2"}), %[
          k1
          k2 v2
        ])
      end

      test "parses attribute key always string" do
        assert_text_parsed_as(e('ROOT', '', {"1" => "1"}), "1 1")
      end

      data("_.%$!,"     => "_.%$!,",
           "/=~-~@\`:?" => "/=~-~@\`:?",
           "()*{}.[]"   => "()*{}.[]")
      test "parses a value with symbols" do |v|
        assert_text_parsed_as(e('ROOT', '', {"k" => v}), "k #{v}")
      end

      test "ignores spacing around value" do
        assert_text_parsed_as(e('ROOT', '', {"k1" => "a"}), "  k1     a    ")
      end

      test "allows spaces in value" do
        assert_text_parsed_as(e('ROOT', '', {"k1" => "a  b  c"}), "k1 a  b  c")
      end

      test "parses value into empty string if only key exists" do
        # value parser parses empty string as true for bool type
        assert_text_parsed_as(e('ROOT', '', {"k1" => ""}), "k1\n")
        assert_text_parsed_as(e('ROOT', '', {"k1" => ""}), "k1")
      end

      sub_test_case 'non-quoted string' do
        test "remains text starting with '#'" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "#not_comment"}), "  k1 #not_comment")
        end

        test "remains text just after '#'" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "a#not_comment"}), "  k1 a#not_comment")
        end

        test "remove text after ` #` (comment)" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "a"}), "  k1 a #comment")
        end

        test "does not require escaping backslash" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "\\\\"}), "  k1 \\\\")
          assert_text_parsed_as(e('ROOT', '', {"k1" => "\\"}), "  k1 \\")
        end

        test "remains backslash in front of a normal character" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => '\['}), "  k1 \\[")
        end

        test "does not accept escape characters" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => '\n'}), "  k1 \\n")
        end
      end

      sub_test_case 'double quoted string' do
        test "allows # in value" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "a#comment"}), '  k1 "a#comment"')
        end

        test "rejects characters after double quoted string" do
          assert_parse_error('  k1 "a" 1')
        end

        test "requires escaping backslash" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "\\"}), '  k1 "\\\\"')
          assert_parse_error('  k1 "\\"')
        end

        test "requires escaping double quote" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => '"'}), '  k1 "\\""')
          assert_parse_error('  k1 """')
          assert_parse_error('  k1 ""\'')
        end

        test "removes backslash in front of a normal character" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => '['}), '  k1 "\\["')
        end

        test "accepts escape characters" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "\n"}), '  k1 "\\n"')
        end

        test "support multiline string" do
          assert_text_parsed_as(e('ROOT', '',
            {"k1" => %[line1
                       line2]
            }),
            %[k1      "line1
                       line2"]
          )
          assert_text_parsed_as(e('ROOT', '',
            {"k1" => %[line1                       line2]
            }),
            %[k1      "line1\\
                       line2"]
          )
          assert_text_parsed_as(e('ROOT', '',
            {"k1" => %[line1
                       line2
                       line3]
            }),
            %[k1      "line1
                       line2
                       line3"]
          )
          assert_text_parsed_as(e('ROOT', '',
            {"k1" => %[line1
                       line2                       line3]
            }),
            %[k1      "line1
                       line2\\
                       line3"]
          )
        end
      end

      sub_test_case 'single quoted string' do
        test "allows # in value" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "a#comment"}), "  k1 'a#comment'")
        end

        test "rejects characters after single quoted string" do
          assert_parse_error("  k1 'a' 1")
        end

        test "requires escaping backslash" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "\\"}), "  k1 '\\\\'")
          assert_parse_error("  k1 '\\'")
        end

        test "requires escaping single quote" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "'"}), "  k1 '\\''")
          assert_parse_error("  k1 '''")
        end

        test "remains backslash in front of a normal character" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => '\\['}), "  k1 '\\['")
        end

        test "does not accept escape characters" do
          assert_text_parsed_as(e('ROOT', '', {"k1" => "\\n"}), "  k1 '\\n'")
        end
      end

      data(
        "in match" => %[
          <match>
            @k v
          </match>
        ],
        "in source" => %[
          <source>
            @k v
          </source>
        ],
        "in filter" => %[
          <filter>
            @k v
          </filter>
        ],
        "in top-level" => '  @k v '
        )
      def test_rejects_at_prefix_in_the_parameter_name(data)
        assert_parse_error(data)
      end

      data(
        "in nested" => %[
          <match>
            <record>
              @k v
            </record>
          </match>
        ]
        )
      def test_not_reject_at_prefix_in_the_parameter_name(data)
        assert_nothing_raised { parse_text(data) }
      end
    end

    sub_test_case 'element parsing' do
      data(
        'root' => [root, ""],
        "accepts empty element" => [root(e("test")), %[
          <test>
          </test>
        ]],
        "accepts argument and attributes" => [root(e("test", 'var', {'key'=>"val"})), %[
          <test var>
            key val
          </test>
        ]],
        "accepts nested elements" => [root(
          e("test", 'var', {'key'=>'1'}, [
            e('nested1'),
            e('nested2')
          ])), %[
          <test var>
            key 1
            <nested1>
            </nested1>
            <nested2>
            </nested2>
          </test>
        ]],
        "accepts multiline json values" => [root(e("test", 'var', {'key'=>"[\"a\",\"b\",\"c\",\"d\"]"})), %[
          <test var>
            key ["a",
"b", "c",
"d"]
          </test>
        ]],
        "parses empty element argument to nil" => [root(e("test", '')), %[
          <test >
          </test>
        ]],
        "ignores spacing around element argument" => [root(e("test", "a")), %[
          <test    a    >
          </test>
        ]],
        "accepts spacing inside element argument (for multiple tags)" => [root(e("test", "a.** b.**")), %[
          <test    a.** b.** >
          </test>
        ]])
      def test_parse_element(data)
        expected, target = data
        assert_text_parsed_as(expected, target)
      end

      [
        "**",
        "*.*",
        "1",
        "_.%$!",
        "/",
        "()*{}.[]",
      ].each do |arg|
        test "parses symbol element argument:#{arg}" do
          assert_text_parsed_as(root(e("test", arg)), %[
            <test #{arg}>
            </test>
          ])
        end
      end

      data(
        "considers comments in element argument" => %[
          <test #a>
          </test>
        ],
        "requires line_end after begin tag" => %[
          <test></test>
        ],
        "requires line_end after end tag" => %[
          <test>
          </test><test>
          </test>
        ])
      def test_parse_error(data)
        assert_parse_error(data)
      end
    end

    # port from test_config.rb
    sub_test_case '@include parsing' do
      TMP_DIR = File.dirname(__FILE__) + "/tmp/v1_config#{ENV['TEST_ENV_NUMBER']}"

      def write_config(path, data)
        FileUtils.mkdir_p(File.dirname(path))
        File.open(path, "w") { |f| f.write data }
      end

      def prepare_config
        write_config "#{TMP_DIR}/config_test_1.conf", %[
        k1 root_config
        include dir/config_test_2.conf  #
        @include #{TMP_DIR}/config_test_4.conf
        include file://#{TMP_DIR}/config_test_5.conf
        @include config.d/*.conf
      ]
        write_config "#{TMP_DIR}/dir/config_test_2.conf", %[
        k2 relative_path_include
        @include ../config_test_3.conf
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
          @include ../dir/config_test_9.conf
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

      test 'parses @include / include correctly' do
        prepare_config
        c = read_config("#{TMP_DIR}/config_test_1.conf")
        assert_equal('root_config', c['k1'])
        assert_equal('relative_path_include', c['k2'])
        assert_equal('relative_include_in_included_file', c['k3'])
        assert_equal('absolute_path_include', c['k4'])
        assert_equal('uri_include', c['k5'])
        assert_equal('wildcard_include_1', c['k6'])
        assert_equal('wildcard_include_2', c['k7'])
        assert_equal('wildcard_include_3', c['k8'])
        assert_equal([
            'k1',
            'k2',
            'k3',
            'k4',
            'k5',
            'k8', # Because of the file name this comes first.
            'k6',
            'k7',
          ], c.keys)

        elem1 = c.elements.find { |e| e.name == 'elem1' }
        assert(elem1)
        assert_equal('name', elem1.arg)
        assert_equal('normal_parameter', elem1['include'])

        elem2 = c.elements.find { |e| e.name == 'elem2' }
        assert(elem2)
        assert_equal('name', elem2.arg)
        assert_equal('embedded', elem2['k9'])
        assert_not_include(elem2, 'include')

        elem3 = elem2.elements.find { |e| e.name == 'elem3' }
        assert(elem3)
        assert_equal('nested_value', elem3['nested'])
        assert_equal('hoge', elem3['include'])
      end

      # TODO: Add uri based include spec
    end

    sub_test_case '#to_s' do
      test 'parses dumpped configuration' do
        original = %q!a\\\n\r\f\b'"z!
        expected = %q!a\\\n\r\f\b'"z!

        conf = parse_text(%[k1 #{original}])
        assert_equal(expected, conf['k1']) # escape check
        conf2 = parse_text(conf.to_s) # use dumpped configuration to check unescape
        assert_equal(expected, conf2.elements.first['k1'])
      end

      test 'all types' do
        conf = parse_text(%[
          param_string "value"
          param_enum foo
          param_integer 999
          param_float 55.55
          param_size 4k
          param_bool true
          param_time 10m
          param_hash { "key1": "value1", "key2": 2 }
          param_array ["value1", "value2", 100]
          param_regexp /pattern/
        ])
        target = AllTypes.new.configure(conf)
        assert_equal(conf.to_s, target.config.to_s)
        expected = <<DUMP
<ROOT>
  param_string "value"
  param_enum foo
  param_integer 999
  param_float 55.55
  param_size 4k
  param_bool true
  param_time 10m
  param_hash {"key1":"value1","key2":2}
  param_array ["value1","value2",100]
  param_regexp /pattern/
</ROOT>
DUMP
        assert_equal(expected, conf.to_s)
      end
    end
  end

  class TestV0Parser < ::Test::Unit::TestCase
    def parse_text(text)
      basepath = File.expand_path(File.dirname(__FILE__) + '/../../')
      Fluent::Config::Parser.parse(StringIO.new(text), '(test)', basepath)
    end

    sub_test_case "Fluent::Config::Element#to_s" do
      test 'all types' do
        conf = parse_text(%[
          param_string value
          param_enum foo
          param_integer 999
          param_float 55.55
          param_size 4k
          param_bool true
          param_time 10m
          param_hash { "key1": "value1", "key2": 2 }
          param_array ["value1", "value2", 100]
          param_regexp /pattern/
        ])
        target = AllTypes.new.configure(conf)
        assert_equal(conf.to_s, target.config.to_s)
        expected = <<DUMP
<ROOT>
  param_string value
  param_enum foo
  param_integer 999
  param_float 55.55
  param_size 4k
  param_bool true
  param_time 10m
  param_hash { "key1": "value1", "key2": 2 }
  param_array ["value1", "value2", 100]
  param_regexp /pattern/
</ROOT>
DUMP
        assert_equal(expected, conf.to_s)
      end
    end
  end
end
