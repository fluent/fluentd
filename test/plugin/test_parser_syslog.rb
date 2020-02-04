require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'

class SyslogParserTest < ::Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    @parser = Fluent::Test::Driver::Parser.new(Fluent::Plugin::SyslogParser)
    @expected = {
      'host'    => '192.168.0.1',
      'ident'   => 'fluentd',
      'pid'     => '11111',
      'message' => '[error] Syslog test'
    }
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse(param)
    @parser.configure({'parser_type' => param})
    @parser.instance.parse('Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected, record)
    }
    assert_equal(Fluent::Plugin::SyslogParser::REGEXP, @parser.instance.patterns['format'])
    assert_equal("%b %d %H:%M:%S", @parser.instance.patterns['time_format'])
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse_with_time_format(param)
    @parser.configure('time_format' => '%b %d %M:%S:%H', 'parser_type' => param)
    @parser.instance.parse('Feb 28 00:00:12 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected, record)
    }
    assert_equal('%b %d %M:%S:%H', @parser.instance.patterns['time_format'])
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse_with_subsecond_time(param)
    @parser.configure('time_format' => '%b %d %H:%M:%S.%N', 'parser_type' => param)
    @parser.instance.parse('Feb 28 12:00:00.456 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
      assert_equal(event_time('Feb 28 12:00:00.456', format: '%b %d %H:%M:%S.%N'), time)
      assert_equal(@expected, record)
    }
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse_with_priority(param)
    @parser.configure('with_priority' => true, 'parser_type' => param)
    @parser.instance.parse('<6>Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected.merge('pri' => 6), record)
    }
    assert_equal(Fluent::Plugin::SyslogParser::REGEXP_WITH_PRI, @parser.instance.patterns['format'])
    assert_equal("%b %d %H:%M:%S", @parser.instance.patterns['time_format'])
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse_with_empty_priority(param)
    @parser.configure('with_priority' => true, 'parser_type' => param)
    @parser.instance.parse('<>Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
      assert_nil time
      assert_nil record
    }
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse_without_colon(param)
    @parser.configure({'parser_type' => param})
    @parser.instance.parse('Feb 28 12:00:00 192.168.0.1 fluentd[11111] [error] Syslog test') { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected, record)
    }
    assert_equal(Fluent::Plugin::SyslogParser::REGEXP, @parser.instance.patterns['format'])
    assert_equal("%b %d %H:%M:%S", @parser.instance.patterns['time_format'])
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse_with_keep_time_key(param)
    @parser.configure(
                      'time_format' => '%b %d %M:%S:%H',
                      'keep_time_key'=>'true',
                      'parser_type' => param
                      )
    text = 'Feb 28 00:00:12 192.168.0.1 fluentd[11111]: [error] Syslog test'
    @parser.instance.parse(text) do |time, record|
      assert_equal "Feb 28 00:00:12", record['time']
    end
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse_various_characters_for_tag(param)
    ident = '~!@#$%^&*()_+=-`]{};"\'/?\\,.<>'
    @parser.configure({'parser_type' => param})
    @parser.instance.parse("Feb 28 12:00:00 192.168.0.1 #{ident}[11111]: [error] Syslog test") { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected.merge('ident' => ident), record)
    }
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse_various_characters_for_tag_with_priority(param)
    ident = '~!@#$%^&*()_+=-`]{};"\'/?\\,.<>'
    @parser.configure('with_priority' => true, 'parser_type' => param)
    @parser.instance.parse("<6>Feb 28 12:00:00 192.168.0.1 #{ident}[11111]: [error] Syslog test") { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected.merge('pri' => 6, 'ident' => ident), record)
    }
  end

  sub_test_case 'Check the difference of regexp and string parser' do
    # examples from rfc3164
    data('regexp' => 'regexp', 'string' => 'string')
    test 'wrong result with no ident message by default' do |param|
      @parser.configure('parser_type' => param)
      @parser.instance.parse('Feb  5 17:32:18 10.0.0.99 Use the BFG!') { |time, record|
        assert_equal({'host' => '10.0.0.99', 'ident' => 'Use', 'message' => 'the BFG!'}, record)
      }
    end

    test "proper result with no ident message by 'support_colonless_ident false'" do
      @parser.configure('parser_type' => 'string', 'support_colonless_ident' => false)
      @parser.instance.parse('Feb  5 17:32:18 10.0.0.99 Use the BFG!') { |time, record|
        assert_equal({'host' => '10.0.0.99', 'message' => 'Use the BFG!'}, record)
      }
    end

    test "string parsers can't parse broken syslog message and generate wrong record" do
      @parser.configure('parser_type' => 'string')
      @parser.instance.parse("1990 Oct 22 10:52:01 TZ-6 scapegoat.dmz.example.org 10.1.2.32 sched[0]: That's All Folks!") { |time, record|
        expected = {'host' => 'scapegoat.dmz.example.org', 'ident' => 'sched', 'pid' => '0', 'message' => "That's All Folks!"}
        assert_not_equal(expected, record)
      }
    end

    test "regexp parsers can't parse broken syslog message and raises an error" do
      @parser.configure('parser_type' => 'regexp')
      assert_raise(Fluent::TimeParser::TimeParseError) {
        @parser.instance.parse("1990 Oct 22 10:52:01 TZ-6 scapegoat.dmz.example.org 10.1.2.32 sched[0]: That's All Folks!") { |time, record| }
      }
    end

    data('regexp' => 'regexp', 'string' => 'string')
    test "':' included message breaks regexp parser" do |param|
      @parser.configure('parser_type' => param)
      @parser.instance.parse('Aug 10 12:00:00 127.0.0.1 test foo:bar') { |time, record|
        expected = {'host' => '127.0.0.1', 'ident' => 'test', 'message' => 'foo:bar'}
        if param == 'string'
          assert_equal(expected, record)
        else
          assert_not_equal(expected, record)
        end
      }
    end

    data('regexp' => 'regexp', 'string' => 'string')
    test "Only no whitespace content in MSG causes different result" do |param|
      @parser.configure('parser_type' => param)
      @parser.instance.parse('Aug 10 12:00:00 127.0.0.1 value1,value2,value3,value4') { |time, record|
        # 'message' is correct but regexp set it as 'ident'
        if param == 'string'
          expected = {'host' => '127.0.0.1', 'message' => 'value1,value2,value3,value4'}
          assert_equal(expected, record)
        else
          expected = {'host' => '127.0.0.1', 'ident' => 'value1,value2,value3,value4', 'message' => ''}
          assert_equal(expected, record)
        end
      }
    end
  end

  class TestRFC5424Regexp < self
    def test_parse_with_rfc5424_message
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])
    end

    def test_parse_with_rfc5424_message_trailing_eol
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = "<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!\n"
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])
    end

    def test_parse_with_rfc5424_multiline_message
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = "<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi,\nfrom\nFluentd!"
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi,\nfrom\nFluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])
    end

    def test_parse_with_rfc5424_message_and_without_priority
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        )
      text = '2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_NO_PRI,
                   @parser.instance.patterns['format'])
    end

    def test_parse_with_rfc5424_empty_message_and_without_priority
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        )
      text = '2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - -'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_nil record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_NO_PRI,
                   @parser.instance.patterns['format'])
    end

    def test_parse_with_rfc5424_message_without_time_format
      @parser.configure(
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
    end

    def test_parse_with_rfc5424_message_with_priority_and_pid
      @parser.configure(
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = '<28>1 2018-09-26T15:54:26.620412+09:00 machine minissdpd 1298 - -  peer 192.168.0.5:50123 is not from a LAN'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2018-09-26T15:54:26.620412+0900", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "1298", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal " peer 192.168.0.5:50123 is not from a LAN", record["message"]
      end
    end

    def test_parse_with_rfc5424_structured_message
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] [Hi] from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "[Hi] from Fluentd!", record["message"]
      end
    end

    def test_parse_with_rfc5424_multiple_structured_message
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"][exampleSDID@20224 class="high"] Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"][exampleSDID@20224 class=\"high\"]",
                     record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
    end

    def test_parse_with_rfc5424_message_includes_right_bracket
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] [Hi] from Fluentd]!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "[Hi] from Fluentd]!", record["message"]
      end
    end

    def test_parse_with_rfc5424_empty_message
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"]'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_nil record["message"]
      end
    end

    def test_parse_with_rfc5424_message_prefixed_unicode_bom
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = "<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"] \xef\xbb\xbf[message]\n"
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "[message]", record["message"]
      end
    end

    def test_parse_with_rfc5424_message_without_subseconds
      @parser.configure(
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = '<16>1 2017-02-06T13:14:15Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15Z", format: '%Y-%m-%dT%H:%M:%S%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
    end

    def test_parse_with_rfc5424_message_both_timestamp
      @parser.configure(
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        )
      text = '<16>1 2017-02-06T13:14:15Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15Z", format: '%Y-%m-%dT%H:%M:%S%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd with subseconds!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd with subseconds!", record["message"]
      end
    end
  end

  class TestAutoRegexp < self
    data('regexp' => 'regexp', 'string' => 'string')
    def test_auto_with_legacy_syslog_message(param)
      @parser.configure(
                        'time_format' => '%b %d %M:%S:%H',
                        'message_format' => 'auto',
                        'parser_type' => param
                        )
      text = 'Feb 28 00:00:12 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 00:00:12", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected, record)
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP, @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_auto_with_legacy_syslog_priority_message(param)
      @parser.configure(
                        'time_format' => '%b %d %M:%S:%H',
                        'with_priority' => true,
                        'message_format' => 'auto',
                        'parser_type' => param
                        )
      text = '<6>Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 12:00:00", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected.merge('pri' => 6), record)
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_WITH_PRI, @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_message(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'auto',
                        'with_priority' => true,
                        'parser_type' => param
                        )
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_structured_message(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'auto',
                        'with_priority' => true,
                        'parser_type' => param
                        )
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_both_message_type(param)
      @parser.configure(
        'time_format' => '%b %d %M:%S:%H',
        'rfc5424_time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
        'message_format' => 'auto',
        'with_priority' => true,
        'parser_type' => param
      )
      text = '<1>Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 12:00:00", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected.merge('pri' => 1), record)
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_WITH_PRI, @parser.instance.patterns['format'])

      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])

      text = '<1>Feb 28 12:00:02 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 12:00:02", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected.merge('pri' => 1), record)
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_WITH_PRI, @parser.instance.patterns['format'])

      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_both_message_type_and_priority(param)
      @parser.configure(
                        'time_format' => '%b %d %M:%S:%H',
                        'rfc5424_time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'with_priority' => true,
                        'message_format' => 'auto',
                        'parser_type' => param
                        )
      text = '<6>Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 12:00:00", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected.merge('pri' => 6), record)
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_WITH_PRI, @parser.instance.patterns['format'])

      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])

      text = '<16>Feb 28 12:00:02 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 12:00:02", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected.merge('pri' => 16), record)
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_WITH_PRI, @parser.instance.patterns['format'])

      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])

      text = '<16>1 2017-02-06T13:14:15Z 192.168.0.1 fluentd - - - Hi, from Fluentd without subseconds!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15Z", format: '%Y-%m-%dT%H:%M:%S%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd without subseconds!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424_WITH_PRI,
                   @parser.instance.patterns['format'])
    end
  end
end
