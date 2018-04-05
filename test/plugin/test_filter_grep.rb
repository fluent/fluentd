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
      d.instance._regexp_and_conditions.each { |value|
        assert_equal(Regexp.compile(/ foo/), value.pattern)
      }
    end

    test "excludeN can contain a space" do
      d = create_driver(%[exclude1 message  foo])
      d.instance._exclude_or_conditions.each { |value|
        assert_equal(Regexp.compile(/ foo/), value.pattern)
      }
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

      test "and/regexp" do
        conf = %[
          <and>
            <regexp>
              key message
              pattern test
            </regexp>
            <regexp>
              key message
              pattern test
            </regexp>
          </and>
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end

      test "and/regexp, and/regexp" do
        conf = %[
          <and>
            <regexp>
              key message
              pattern test
            </regexp>
          </and>
          <and>
            <regexp>
              key message
              pattern test
            </regexp>
          </and>
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end

      test "regexp, and/regexp" do
        conf = %[
          <regexp>
            key message
            pattern test
          </regexp>
          <and>
            <regexp>
              key message
              pattern test
            </regexp>
          </and>
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end

      test "and/exclude" do
        conf = %[
          <and>
            <exclude>
              key message
              pattern test
            </exclude>
            <exclude>
              key message
              pattern test
            </exclude>
          </and>
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end

      test "and/exclude, and/exclude" do
        conf = %[
          <and>
            <exclude>
              key message
              pattern test
            </exclude>
          </and>
          <and>
            <exclude>
              key message
              pattern test
            </exclude>
          </and>
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end

      test "exclude, or/exclude" do
        conf = %[
          <exclude>
            key message
            pattern test
          </exclude>
          <or>
            <exclude>
              key message
              pattern test
            </exclude>
          </or>
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end
    end

    sub_test_case "pattern with slashes" do
      test "start with character classes" do
        conf = %[
          <regexp>
            key message
            pattern /[a-z]test/
          </regexp>
          <exclude>
            key message
            pattern /[A-Z]test/
          </exclude>
        ]
        d = create_driver(conf)
        assert_equal(/[a-z]test/, d.instance.regexps.first.pattern)
        assert_equal(/[A-Z]test/, d.instance.excludes.first.pattern)
      end
    end

    sub_test_case "and/or section" do
      test "<and> section cannot include both <regexp> and <exclude>" do
        conf = %[
          <and>
            <regexp>
              key message
              pattern /test/
            </regexp>
            <exclude>
              key level
              pattern /debug/
            </exclude>
          </and>
        ]
        assert_raise(Fluent::ConfigError) do
          create_driver(conf)
        end
      end

      test "<or> section cannot include both <regexp> and <exclude>" do
        conf = %[
          <or>
            <regexp>
              key message
              pattern /test/
            </regexp>
            <exclude>
              key level
              pattern /debug/
            </exclude>
          </or>
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

    test 'regexps' do
      conf = %[
        <regexp>
          key message
          pattern WARN
        </regexp>
      ]
      filtered_records = filter(conf, messages)
      assert_equal(3, filtered_records.size)
      assert_block('only WARN logs') do
        filtered_records.all? { |r|
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
      filtered_records = filter(conf, messages)
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

    sub_test_case "and/or section" do
      def records
        [
          { "time" => "2013/01/13T07:02:11.124202", "level" => "INFO", "method" => "GET", "path" => "/ping" },
          { "time" => "2013/01/13T07:02:13.232645", "level" => "WARN", "method" => "POST", "path" => "/auth" },
          { "time" => "2013/01/13T07:02:21.542145", "level" => "WARN", "method" => "GET", "path" => "/favicon.ico" },
          { "time" => "2013/01/13T07:02:43.632145", "level" => "WARN", "method" => "POST", "path" => "/login" },
        ]
      end

      def filter(conf, records)
        d = create_driver(conf)
        d.run do
          records.each do |record|
            d.feed("filter.test", @time, record)
          end
        end
        d.filtered_records
      end

      test "basic and/regexp" do
        conf = %[
          <and>
            <regexp>
              key level
              pattern ^INFO$
            </regexp>
            <regexp>
              key method
              pattern ^GET$
            </regexp>
          </and>
        ]
        filtered_records = filter(conf, records)
        assert_equal(records.values_at(0), filtered_records)
      end

      test "basic or/exclude" do
        conf = %[
          <or>
            <exclude>
              key level
              pattern ^INFO$
            </exclude>
            <exclude>
              key method
              pattern ^GET$
            </exclude>
          </or>
        ]
        filtered_records = filter(conf, records)
        assert_equal(records.values_at(1, 3), filtered_records)
      end

      test "basic or/regexp" do
        conf = %[
          <or>
            <regexp>
              key level
              pattern ^INFO$
            </regexp>
            <regexp>
              key method
              pattern ^GET$
            </regexp>
          </or>
        ]
        filtered_records = filter(conf, records)
        assert_equal(records.values_at(0, 2), filtered_records)
      end

      test "basic and/exclude" do
        conf = %[
          <and>
            <exclude>
              key level
              pattern ^INFO$
            </exclude>
            <exclude>
              key method
              pattern ^GET$
            </exclude>
          </and>
        ]
        filtered_records = filter(conf, records)
        assert_equal(records.values_at(1, 2, 3), filtered_records)
      end

      sub_test_case "and/or combo" do
        def records
          [
            { "time" => "2013/01/13T07:02:11.124202", "level" => "INFO", "method" => "GET", "path" => "/ping" },
            { "time" => "2013/01/13T07:02:13.232645", "level" => "WARN", "method" => "POST", "path" => "/auth" },
            { "time" => "2013/01/13T07:02:21.542145", "level" => "WARN", "method" => "GET", "path" => "/favicon.ico" },
            { "time" => "2013/01/13T07:02:43.632145", "level" => "WARN", "method" => "POST", "path" => "/login" },
            { "time" => "2013/01/13T07:02:44.959307", "level" => "ERROR", "method" => "POST", "path" => "/login" },
            { "time" => "2013/01/13T07:02:45.444992", "level" => "ERROR", "method" => "GET", "path" => "/ping" },
            { "time" => "2013/01/13T07:02:51.247941", "level" => "WARN", "method" => "GET", "path" => "/info" },
            { "time" => "2013/01/13T07:02:53.108366", "level" => "WARN", "method" => "POST", "path" => "/ban" },
          ]
        end

        test "and/regexp, or/exclude" do
          conf = %[
            <and>
              <regexp>
                key level
                pattern ^ERROR|WARN$
              </regexp>
              <regexp>
                key method
                pattern ^GET|POST$
              </regexp>
            </and>
            <or>
              <exclude>
                key level
                pattern ^WARN$
              </exclude>
              <exclude>
                key method
                pattern ^GET$
              </exclude>
            </or>
          ]
          filtered_records = filter(conf, records)
          assert_equal(records.values_at(4), filtered_records)
        end

        test "and/regexp, and/exclude" do
          conf = %[
            <and>
              <regexp>
                key level
                pattern ^ERROR|WARN$
              </regexp>
              <regexp>
                key method
                pattern ^GET|POST$
              </regexp>
            </and>
            <and>
              <exclude>
                key level
                pattern ^WARN$
              </exclude>
              <exclude>
                key method
                pattern ^GET$
              </exclude>
            </and>
          ]
          filtered_records = filter(conf, records)
          assert_equal(records.values_at(1, 3, 4, 5, 7), filtered_records)
        end

        test "or/regexp, and/exclude" do
          conf = %[
            <or>
              <regexp>
                key level
                pattern ^ERROR|WARN$
              </regexp>
              <regexp>
                key method
                pattern ^GET|POST$
              </regexp>
            </or>
            <and>
              <exclude>
                key level
                pattern ^WARN$
              </exclude>
              <exclude>
                key method
                pattern ^GET$
              </exclude>
            </and>
          ]
          filtered_records = filter(conf, records)
          assert_equal(records.values_at(0, 1, 3, 4, 5, 7), filtered_records)
        end

        test "or/regexp, or/exclude" do
          conf = %[
            <or>
              <regexp>
                key level
                pattern ^ERROR|WARN$
              </regexp>
              <regexp>
                key method
                pattern ^GET|POST$
              </regexp>
            </or>
            <or>
              <exclude>
                key level
                pattern ^WARN$
              </exclude>
              <exclude>
                key method
                pattern ^GET$
              </exclude>
            </or>
          ]
          filtered_records = filter(conf, records)
          assert_equal(records.values_at(4), filtered_records)
        end

        test "regexp, and/regexp" do
          conf = %[
            <and>
              <regexp>
                key level
                pattern ^ERROR|WARN$
              </regexp>
              <regexp>
                key method
                pattern ^GET|POST$
              </regexp>
            </and>
            <regexp>
              key path
              pattern ^/login$
            </regexp>
          ]
          filtered_records = filter(conf, records)
          assert_equal(records.values_at(3, 4), filtered_records)
        end

        test "regexp, or/exclude" do
          conf = %[
            <regexp>
              key level
              pattern ^ERROR|WARN$
            </regexp>
            <regexp>
              key method
              pattern ^GET|POST$
            </regexp>
            <or>
              <exclude>
                key level
                pattern ^WARN$
              </exclude>
              <exclude>
                key method
                pattern ^GET$
              </exclude>
            </or>
          ]
          filtered_records = filter(conf, records)
          assert_equal(records.values_at(4), filtered_records)
        end

        test "regexp, and/exclude" do
          conf = %[
            <regexp>
              key level
              pattern ^ERROR|WARN$
            </regexp>
            <regexp>
              key method
              pattern ^GET|POST$
            </regexp>
            <and>
              <exclude>
                key level
                pattern ^WARN$
              </exclude>
              <exclude>
                key method
                pattern ^GET$
              </exclude>
            </and>
          ]
          filtered_records = filter(conf, records)
          assert_equal(records.values_at(1, 3, 4, 5, 7), filtered_records)
        end
      end
    end
  end

  sub_test_case 'nested keys' do
    def messages
      [
        {"nest1" => {"nest2" => "INFO"}},
        {"nest1" => {"nest2" => "WARN"}},
        {"nest1" => {"nest2" => "WARN"}}
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

    test 'regexps' do
      conf = %[
        <regexp>
          key $.message.nest1.nest2
          pattern WARN
        </regexp>
      ]
      filtered_records = filter(conf, messages)
      assert_equal(2, filtered_records.size)
      assert_block('only 2 nested logs') do
        filtered_records.all? { |r|
          r['message']['nest1']['nest2'] == 'WARN'
        }
      end
    end

    test 'excludes' do
      conf = %[
        <exclude>
          key $.message.nest1.nest2
          pattern WARN
        </exclude>
      ]
      filtered_records = filter(conf, messages)
      assert_equal(1, filtered_records.size)
      assert_block('only 2 nested logs') do
        filtered_records.all? { |r|
          r['message']['nest1']['nest2'] == 'INFO'
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
