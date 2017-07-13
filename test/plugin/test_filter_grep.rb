require_relative '../helper'
require 'fluent/plugin/filter_grep'

class GrepFilterTest < Test::Unit::TestCase
  include Fluent

  setup do
    Fluent::Test.setup
    @time = Fluent::Engine.now
  end

  def create_driver(conf = '')
    Test::FilterTestDriver.new(GrepFilter).configure(conf, true)
  end

  sub_test_case 'configure' do
    test 'check default' do
      d = create_driver
      assert_empty(d.instance.regexps)
      assert_empty(d.instance.excludes)
    end

    test "regexpN can contain a space" do
      d = create_driver(%[regexp1 message  foo])
      assert_equal(Regexp.compile(/ foo/), d.instance._regexps['message'])
    end

    test "excludeN can contain a space" do
      d = create_driver(%[exclude1 message  foo])
      assert_equal(Regexp.compile(/ foo/), d.instance._excludes['message'])
    end

    sub_test_case "duplicate key" do
      test "flat" do
        conf = %[
          regexp1 message test
          regexp2 message test2
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end
      test "section" do
        conf = %[
          <regexp>
            key message
            pattern test
          </regexp>
          <regexp>
            key message
            pattern test2
          </regexp>
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end
      test "mix" do
        conf = %[
          regexp1 message test
          <regexp>
            key message
            pattern test
          </regexp>
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end
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

    def emit(config, msgs)
      d = create_driver(config)
      d.run {
        msgs.each { |msg|
          d.emit({'foo' => 'bar', 'message' => msg}, @time)
        }
      }.filtered
    end

    test 'empty config' do
      es = emit('', messages)
      assert_equal(4, es.instance_variable_get(:@record_array).size)
    end

    test 'regexpN' do
      es = emit('regexp1 message WARN', messages)
      assert_equal(3, es.instance_variable_get(:@record_array).size)
      assert_block('only WARN logs') do
        es.all? { |t, r|
          !r['message'].include?('INFO')
        }
      end
    end

    test 'excludeN' do
      es = emit('exclude1 message favicon', messages)
      assert_equal(3, es.instance_variable_get(:@record_array).size)
      assert_block('remove favicon logs') do
        es.all? { |t, r|
          !r['message'].include?('favicon')
        }
      end
    end

    test 'regexps' do
      conf = %[
        <regexp>
          key message
          pattern WARN
        </regexp>
      ]
      es = emit(conf, messages)
      assert_equal(3, es.instance_variable_get(:@record_array).size)
      assert_block('only WARN logs') do
        es.all? { |t, r|
          !r['message'].include?('INFO')
        }
      end
    end

    test 'excludes' do
      conf = %[
        <exclude>
          key message
          pattern favicon
        </exclude>
      ]
      es = emit(conf, messages)
      assert_equal(3, es.instance_variable_get(:@record_array).size)
      assert_block('remove favicon logs') do
        es.all? { |t, r|
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
          emit(%[regexp1 message WARN], ["\xff".force_encoding('UTF-8')])
        }
      end
    end
  end

  sub_test_case 'grep non-string jsonable values' do
    def emit(msg, config = 'regexp1 message 0')
      d = create_driver(config)
      d.emit({'foo' => 'bar', 'message' => msg}, @time)
      d.run.filtered
    end

    data(
      'array' => ["0"],
      'hash' => ["0" => "0"],
      'integer' => 0,
      'float' => 0.1)
    test "value" do |data|
      es = emit(data)
      assert_equal(1, es.instance_variable_get(:@record_array).size)
    end

    test "value boolean" do
      es = emit(true, %[regexp1 message true])
      assert_equal(1, es.instance_variable_get(:@record_array).size)
    end
  end
end
