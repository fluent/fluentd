require_relative '../helper'
require 'fluent/plugin/filter_grep'
require 'fluent/test/driver/filter'

class GrepFilterTest < Test::Unit::TestCase
  include Fluent

  setup do
    Fluent::Test.setup
    @time = event_time
  end

  def create_driver(conf = '')
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::GrepFilter).configure(conf)
  end

  sub_test_case 'configure' do
    test 'check default' do
      d = create_driver
      assert_empty(d.instance.regexps)
      assert_empty(d.instance.excludes)
    end

    test "regexpN can contain a space" do
      d = create_driver(%[regexp1 message  foo])
      assert_equal(Regexp.compile(/ foo/), d.instance.regexps['message'])
    end

    test "excludeN can contain a space" do
      d = create_driver(%[exclude1 message  foo])
      assert_equal(Regexp.compile(/ foo/), d.instance.excludes['message'])
    end
  end

  sub_test_case 'filter_stream' do
    def messages
      [
        "2013/01/13T07:02:11.124202 INFO GET /ping",
        "2013/01/13T07:02:13.232645 WARN POST /auth",
        "2013/01/13T07:02:21.542145 WARN GET /favicon.ico",
        "2013/01/13T07:02:43.632145 WARN POST /login",
      ]
    end

    def filter(config, msgs)
      d = create_driver(config)
      d.run {
        msgs.each { |msg|
          d.feed("filter.test", @time, {'foo' => 'bar', 'message' => msg})
        }
      }
      d.filtered_records
    end

    test 'empty config' do
      filtered_records = filter('', messages)
      assert_equal(4, filtered_records.size)
    end

    test 'regexpN' do
      filtered_records = filter('regexp1 message WARN', messages)
      assert_equal(3, filtered_records.size)
      assert_block('only WARN logs') do
        filtered_records.all? { |r|
          !r['message'].include?('INFO')
        }
      end
    end

    test 'excludeN' do
      filtered_records = filter('exclude1 message favicon', messages)
      assert_equal(3, filtered_records.size)
      assert_block('remove favicon logs') do
        filtered_records.all? { |r|
          !r['message'].include?('favicon')
        }
      end
    end

    sub_test_case 'with invalid sequence' do
      def messages
        [
          "\xff".force_encoding('UTF-8'),
        ]
      end

      test "don't raise an exception" do
        assert_nothing_raised { 
          filter(%[regexp1 message WARN], ["\xff".force_encoding('UTF-8')])
        }
      end
    end
  end

  sub_test_case 'grep non-string jsonable values' do
    def filter(msg, config = 'regexp1 message 0')
      d = create_driver(config)
      d.run do
        d.feed("filter.test", @time, {'foo' => 'bar', 'message' => msg})
      end
      d.filtered_records
    end

    data(
      'array' => ["0"],
      'hash' => ["0" => "0"],
      'integer' => 0,
      'float' => 0.1)
    test "value" do |data|
      filtered_records = filter(data)
      assert_equal(1, filtered_records.size)
    end

    test "value boolean" do
      filtered_records = filter(true, %[regexp1 message true])
      assert_equal(1, filtered_records.size)
    end
  end
end
