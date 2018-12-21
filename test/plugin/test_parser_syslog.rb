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

  def test_parse
    @parser.configure({})
    @parser.instance.parse('Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected, record)
    }
    assert_equal(Fluent::Plugin::SyslogParser::REGEXP, @parser.instance.patterns['format'])
    assert_equal("%b %d %H:%M:%S", @parser.instance.patterns['time_format'])
  end

  def test_parse_with_time_format
    @parser.configure('time_format' => '%b %d %M:%S:%H')
    @parser.instance.parse('Feb 28 00:00:12 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected, record)
    }
    assert_equal('%b %d %M:%S:%H', @parser.instance.patterns['time_format'])
  end

  def test_parse_with_priority
    @parser.configure('with_priority' => true)
    @parser.instance.parse('<6>Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test') { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected.merge('pri' => 6), record)
    }
    assert_equal(Fluent::Plugin::SyslogParser::REGEXP_WITH_PRI, @parser.instance.patterns['format'])
    assert_equal("%b %d %H:%M:%S", @parser.instance.patterns['time_format'])
  end

  def test_parse_without_colon
    @parser.configure({})
    @parser.instance.parse('Feb 28 12:00:00 192.168.0.1 fluentd[11111] [error] Syslog test') { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected, record)
    }
    assert_equal(Fluent::Plugin::SyslogParser::REGEXP, @parser.instance.patterns['format'])
    assert_equal("%b %d %H:%M:%S", @parser.instance.patterns['time_format'])
  end

  def test_parse_with_keep_time_key
    @parser.configure(
                      'time_format' => '%b %d %M:%S:%H',
                      'keep_time_key'=>'true',
                      )
    text = 'Feb 28 00:00:12 192.168.0.1 fluentd[11111]: [error] Syslog test'
    @parser.instance.parse(text) do |time, record|
      assert_equal "Feb 28 00:00:12", record['time']
    end
  end

  def test_parse_various_characters_for_tag
    ident = '~!@#$%^&*()_+=-`]{};"\'/?\\,.<>'
    @parser.configure({})
    @parser.instance.parse("Feb 28 12:00:00 192.168.0.1 #{ident}[11111]: [error] Syslog test") { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected.merge('ident' => ident), record)
    }
  end

  def test_parse_various_characters_for_tag_with_priority
    ident = '~!@#$%^&*()_+=-`]{};"\'/?\\,.<>'
    @parser.configure('with_priority' => true)
    @parser.instance.parse("<6>Feb 28 12:00:00 192.168.0.1 #{ident}[11111]: [error] Syslog test") { |time, record|
      assert_equal(event_time('Feb 28 12:00:00', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected.merge('pri' => 6, 'ident' => ident), record)
    }
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
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424,
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
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_RFC5424,
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
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
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
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] Hi, from Fluentd]!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "Hi, from Fluentd]!", record["message"]
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
    def test_auto_with_legacy_syslog_message
      @parser.configure(
                        'time_format' => '%b %d %M:%S:%H',
                        'mseeage_format' => 'auto',
                        )
      text = 'Feb 28 00:00:12 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 00:00:12", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected, record)
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP, @parser.instance.patterns['format'])
    end

    def test_auto_with_legacy_syslog_priority_message
      @parser.configure(
                        'time_format' => '%b %d %M:%S:%H',
                        'with_priority' => true,
                        'mseeage_format' => 'auto',
                        )
      text = '<6>Feb 28 12:00:00 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 12:00:00", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected.merge('pri' => 6), record)
      end
      assert_equal(Fluent::Plugin::SyslogParser::REGEXP_WITH_PRI, @parser.instance.patterns['format'])
    end

    def test_parse_with_rfc5424_message
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'auto',
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

    def test_parse_with_rfc5424_structured_message
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'auto',
                        'with_priority' => true,
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

    def test_parse_with_both_message_type
      @parser.configure(
        'time_format' => '%b %d %M:%S:%H',
        'rfc5424_time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
        'message_format' => 'auto',
        'with_priority' => true,
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

    def test_parse_with_both_message_type_and_priority
      @parser.configure(
                        'time_format' => '%b %d %M:%S:%H',
                        'rfc5424_time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'with_priority' => true,
                        'message_format' => 'auto',
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
