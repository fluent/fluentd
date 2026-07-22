require_relative '../helper'
require 'fluent/test/driver/parser'
require 'fluent/plugin/parser'
require 'timeout'

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
    assert_equal(Fluent::Plugin::SyslogParser::RFC3164_WITHOUT_TIME_AND_PRI_REGEXP, @parser.instance.patterns['format'])
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
  def test_parse_with_time_format2(param)
    @parser.configure('time_format' => '%Y-%m-%dT%H:%M:%SZ', 'parser_type' => param)
    @parser.instance.parse("#{Time.now.year}-03-03T10:14:29Z 192.168.0.1 fluentd[11111]: [error] Syslog test") { |time, record|
      assert_equal(event_time('Mar 03 10:14:29', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected, record)
    }
    assert_equal('%Y-%m-%dT%H:%M:%SZ', @parser.instance.patterns['time_format'])
  end

  def test_parse_with_time_format_rfc5424
    @parser.configure('time_format' => '%Y-%m-%dT%H:%M:%SZ', 'message_format' => 'rfc5424')
    @parser.instance.parse("#{Time.now.year}-03-03T10:14:29Z 192.168.0.1 fluentd 11111 - - [error] Syslog test") { |time, record|
      assert_equal(event_time('Mar 03 10:14:29', format: '%b %d %H:%M:%S'), time)
      assert_equal(@expected.merge('host' => '192.168.0.1', 'msgid' => '-', 'extradata' => '-'), record)
    }
    assert_equal('%Y-%m-%dT%H:%M:%SZ', @parser.instance.patterns['time_format'])
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
    assert_equal(Fluent::Plugin::SyslogParser::RFC3164_WITHOUT_TIME_AND_PRI_REGEXP, @parser.instance.patterns['format'])
    assert_equal("%b %d %H:%M:%S", @parser.instance.patterns['time_format'])
  end

  data(
    'regexp/rfc3164/parser priority' => ['regexp', 'rfc3164', true],
    'string/rfc3164/parser priority' => ['string', 'rfc3164', true],
    'regexp/auto/parser priority' => ['regexp', 'auto', true],
    'string/auto/parser priority' => ['string', 'auto', true],
    'regexp/rfc3164/without priority' => ['regexp', 'rfc3164', false],
    'string/rfc3164/without priority' => ['string', 'rfc3164', false],
    'regexp/auto/without priority' => ['regexp', 'auto', false],
    'string/auto/without priority' => ['string', 'auto', false],
  )
  def test_parse_with_space_between_rfc3164_priority_and_header(data)
    parser_engine, message_format, with_priority = data
    @parser.configure(
      'parser_engine' => parser_engine,
      'message_format' => message_format,
      'with_priority' => with_priority,
      'keep_time_key' => true,
    )

    prefix = with_priority ? '<14>' : ''
    message = 'Apr 25 16:43:29 PAA-SW1-1 General[procLOG]: main.c(257) 272264 %% Stopping System API  application'
    results = ["#{prefix}#{message}", "#{prefix} #{message}"].map do |text|
      result = nil
      @parser.instance.parse(text) do |time, record|
        result = [time, record]
      end
      result
    end

    assert_equal(results[0], results[1])
    time, record = results[1]
    assert_equal(event_time('Apr 25 16:43:29', format: '%b %d %H:%M:%S'), time)
    assert_equal(14, record['pri']) if with_priority
    assert_equal('Apr 25 16:43:29', record['time'])
    assert_equal('PAA-SW1-1', record['host'])
    assert_equal('General', record['ident'])
    assert_equal('main.c(257) 272264 %% Stopping System API  application', record['message'])
  end

  data(
    'regexp/single digit day/with priority' => ['regexp', true, '%b %d %H:%M:%S', 'Apr  5 16:43:29'],
    'string/single digit day/with priority' => ['string', true, '%b %d %H:%M:%S', 'Apr  5 16:43:29'],
    'regexp/subseconds/with priority' => ['regexp', true, '%b %d %H:%M:%S.%N', 'Apr 25 16:43:29.123456789'],
    'string/subseconds/with priority' => ['string', true, '%b %d %H:%M:%S.%N', 'Apr 25 16:43:29.123456789'],
    'regexp/custom format/with priority' => ['regexp', true, '%Y-%m-%dT%H:%M:%S', '2026-04-25T16:43:29'],
    'string/custom format/with priority' => ['string', true, '%Y-%m-%dT%H:%M:%S', '2026-04-25T16:43:29'],
    'regexp/single digit day/without priority' => ['regexp', false, '%b %d %H:%M:%S', 'Apr  5 16:43:29'],
    'string/single digit day/without priority' => ['string', false, '%b %d %H:%M:%S', 'Apr  5 16:43:29'],
    'regexp/subseconds/without priority' => ['regexp', false, '%b %d %H:%M:%S.%N', 'Apr 25 16:43:29.123456789'],
    'string/subseconds/without priority' => ['string', false, '%b %d %H:%M:%S.%N', 'Apr 25 16:43:29.123456789'],
    'regexp/custom format/without priority' => ['regexp', false, '%Y-%m-%dT%H:%M:%S', '2026-04-25T16:43:29'],
    'string/custom format/without priority' => ['string', false, '%Y-%m-%dT%H:%M:%S', '2026-04-25T16:43:29'],
  )
  def test_parse_rfc3164_time_variants_with_space_after_priority(data)
    parser_engine, with_priority, time_format, timestamp = data
    @parser.configure(
      'parser_engine' => parser_engine,
      'time_format' => time_format,
      'with_priority' => with_priority,
      'keep_time_key' => true,
    )

    suffix = "#{timestamp} host app[123]: message"
    prefix = with_priority ? '<14>' : ''
    results = ["#{prefix}#{suffix}", "#{prefix} #{suffix}"].map do |text|
      result = nil
      @parser.instance.parse(text) do |time, record|
        result = [time, record]
      end
      result
    end

    assert_equal(results[0], results[1])
    expected_time_key = parser_engine == 'regexp' ? timestamp.squeeze(' ') : timestamp
    assert_equal(expected_time_key, results[1][1]['time'])
    assert_equal('host', results[1][1]['host'])
    assert_equal('app', results[1][1]['ident'])
    assert_equal('123', results[1][1]['pid'])
    assert_equal('message', results[1][1]['message'])
  end

  data(
    'regexp/parser priority' => ['regexp', true],
    'string/parser priority' => ['string', true],
    'regexp/input priority' => ['regexp', false],
    'string/input priority' => ['string', false],
  )
  def test_parse_with_leading_space_time_format(data)
    parser_engine, with_priority = data
    @parser.configure(
      'parser_engine' => parser_engine,
      'time_format' => ' %b %d %H:%M:%S',
      'with_priority' => with_priority,
      'keep_time_key' => true,
    )

    prefix = with_priority ? '<14>' : ''
    result = nil
    @parser.instance.parse("#{prefix} Apr 25 16:43:29 host app[123]: message") do |time, record|
      result = [time, record]
    end

    time, record = result
    assert_equal(event_time('Apr 25 16:43:29', format: '%b %d %H:%M:%S'), time)
    assert_equal(14, record['pri']) if with_priority
    assert_equal(' Apr 25 16:43:29', record['time'])
    assert_equal(
      {'host' => 'host', 'ident' => 'app', 'pid' => '123', 'message' => 'message'},
      record.reject { |key, _| ['pri', 'time'].include?(key) },
    )
  end

  data(
    'regexp/two spaces' => ['regexp', true, '<14>  Apr 25 16:43:29 host app: message'],
    'string/two spaces' => ['string', true, '<14>  Apr 25 16:43:29 host app: message'],
    'regexp/tab' => ['regexp', true, "<14>\tApr 25 16:43:29 host app: message"],
    'string/tab' => ['string', true, "<14>\tApr 25 16:43:29 host app: message"],
    'regexp/two leading spaces' => ['regexp', false, '  Apr 25 16:43:29 host app: message'],
    'string/two leading spaces' => ['string', false, '  Apr 25 16:43:29 host app: message'],
    'regexp/leading tab' => ['regexp', false, "\tApr 25 16:43:29 host app: message"],
    'string/leading tab' => ['string', false, "\tApr 25 16:43:29 host app: message"],
    'regexp/long priority' => ['regexp', true, '<1234> Apr 25 16:43:29 host app: message'],
    'string/long priority' => ['string', true, '<1234> Apr 25 16:43:29 host app: message'],
    'regexp/non-numeric priority' => ['regexp', true, '<ab> Apr 25 16:43:29 host app: message'],
    'string/non-numeric priority' => ['string', true, '<ab> Apr 25 16:43:29 host app: message'],
  )
  def test_parse_does_not_broaden_rfc3164_priority_separator(data)
    parser_engine, with_priority, text = data
    @parser.configure('parser_engine' => parser_engine, 'with_priority' => with_priority)
    parsed = false

    begin
      @parser.instance.parse(text) do |time, record|
        parsed = !!(time && record)
      end
    rescue Fluent::TimeParser::TimeParseError
      # The regexp and string parsers report invalid input differently.
    end

    assert_false(parsed)
  end

  data(
    'zero' => ['<0> Apr 25 16:43:29 host app: message', 0],
    'two digits with leading zero' => ['<01> Apr 25 16:43:29 host app: message', 1],
    'three digits with leading zeros' => ['<001> Apr 25 16:43:29 host app: message', 1],
  )
  def test_string_parser_accepts_spaced_numeric_priority_with_leading_zeros(data)
    text, expected_priority = data
    @parser.configure('parser_engine' => 'string', 'with_priority' => true)
    result = nil
    @parser.instance.parse(text) do |time, record|
      result = [time, record]
    end

    time, record = result
    assert_not_nil(time)
    assert_equal(expected_priority, record['pri'])
    assert_equal('host', record['host'])
    assert_equal('app', record['ident'])
    assert_equal('message', record['message'])
  end

  data(
    'long priority' => '<1234> Apr 25 16:43:29 host app: message',
    'non-numeric priority' => '<ab> Apr 25 16:43:29 host app: message',
    'partially numeric priority' => '<1a> Apr 25 16:43:29 host app: message',
    'non-ASCII priority' => '<é> Apr 25 16:43:29 host app: message',
  )
  def test_leading_space_time_format_does_not_broaden_priority_syntax(text)
    @parser.configure(
      'parser_engine' => 'string',
      'time_format' => ' %b %d %H:%M:%S',
      'with_priority' => true,
    )
    parsed = false
    @parser.instance.parse(text) do |time, record|
      parsed = !!(time && record)
    end

    assert_false(parsed)
  end

  data(
    'regexp/rfc5424/with priority' => ['regexp', 'rfc5424', true],
    'string/rfc5424/with priority' => ['string', 'rfc5424', true],
    'regexp/auto/with priority' => ['regexp', 'auto', true],
    'string/auto/with priority' => ['string', 'auto', true],
    'regexp/rfc5424/without priority' => ['regexp', 'rfc5424', false],
    'string/rfc5424/without priority' => ['string', 'rfc5424', false],
    'regexp/auto/without priority' => ['regexp', 'auto', false],
    'string/auto/without priority' => ['string', 'auto', false],
  )
  def test_parse_does_not_accept_space_after_rfc5424_priority(data)
    parser_engine, message_format, with_priority = data
    @parser.configure(
      'parser_engine' => parser_engine,
      'message_format' => message_format,
      'with_priority' => with_priority,
    )
    text = with_priority ? '<14> 1 2026-04-25T16:43:29Z host app 123 ID - message' : ' 1 2026-04-25T16:43:29Z host app 123 ID - message'
    parsed = false

    begin
      @parser.instance.parse(text) do |time, record|
        parsed = !!(time && record)
      end
    rescue Fluent::TimeParser::TimeParseError
      # The regexp and string parsers report invalid input differently.
    end

    assert_false(parsed)
  end

  data('regexp' => 'regexp', 'string' => 'string')
  def test_parse_rfc5452_with_priority(param)
    @parser.configure('with_priority' => true, 'parser_type' => param, 'message_format' => 'rfc5424')
    @parser.instance.parse('<30>1 2020-03-31T20:32:54Z myhostname 02abaf0687f5 10339 02abaf0687f5 - method=POST db=0.00') do |time, record|
      assert_equal(event_time('2020-03-31T20:32:54Z', format: '%Y-%m-%dT%H:%M:%S%z'), time)
      expected = { 'extradata' => '-', 'host' => 'myhostname', 'ident' => '02abaf0687f5', 'message' => 'method=POST db=0.00', 'msgid' => '02abaf0687f5', 'pid' => '10339', 'pri' => 30 }
      assert_equal(expected, record)
    end
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
    assert_equal(Fluent::Plugin::SyslogParser::RFC3164_WITHOUT_TIME_AND_PRI_REGEXP, @parser.instance.patterns['format'])
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
    def test_rejects_invalid_structured_data_without_excessive_backtracking
      text = 'host ident pid msgid ' + ('[example]' * 30) + 'invalid'

      match = Timeout.timeout(1) do
        Fluent::Plugin::SyslogParser::RFC5424_WITHOUT_TIME_AND_PRI_REGEXP.match(text)
      end

      assert_nil(match)
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_message(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
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
      assert_equal(Fluent::Plugin::SyslogParser::RFC5424_WITHOUT_TIME_AND_PRI_REGEXP, @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_message_trailing_eol(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
                        )
      text = "<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!\n"
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::RFC5424_WITHOUT_TIME_AND_PRI_REGEXP, @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_multiline_message(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
                        )
      text = "<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi,\nfrom\nFluentd!"
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi,\nfrom\nFluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::RFC5424_WITHOUT_TIME_AND_PRI_REGEXP, @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_message_and_without_priority(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'parser_type' => param
                        )
      text = '2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::RFC5424_WITHOUT_TIME_AND_PRI_REGEXP, @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_empty_message_and_without_priority(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'parser_type' => param
                        )
      text = '2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - -'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_nil record["message"]
      end
      assert_equal(Fluent::Plugin::SyslogParser::RFC5424_WITHOUT_TIME_AND_PRI_REGEXP, @parser.instance.patterns['format'])
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_message_without_time_format(param)
      @parser.configure(
                        'message_format' => 'rfc5424',
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
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_message_with_priority_and_pid(param)
      @parser.configure(
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
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

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_structured_message(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
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

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_multiple_structured_message(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
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

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_message_includes_right_bracket(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
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

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_empty_message(param)
      @parser.configure(
                        'time_format' => '%Y-%m-%dT%H:%M:%S.%L%z',
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
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

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_space_empty_message(param)
      @parser.configure(
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
                        )
      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] '
      @parser.instance.parse(text) do |time, record|
        if param == 'string'
          assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
          assert_equal "11111", record["pid"]
          assert_equal "ID24224", record["msgid"]
          assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                       record["extradata"]
          assert_equal '', record["message"]
        else
          assert_nil time
          assert_nil record
        end
      end
    end

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_message_without_subseconds(param)
      @parser.configure(
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
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

    data('regexp' => 'regexp', 'string' => 'string')
    def test_parse_with_rfc5424_message_both_timestamp(param)
      @parser.configure(
                        'message_format' => 'rfc5424',
                        'with_priority' => true,
                        'parser_type' => param
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
        assert_equal 16, record["pri"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
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

      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end

      text = '<1>Feb 28 12:00:02 192.168.0.1 fluentd[11111]: [error] Syslog test 2>1'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 12:00:02", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected.merge('pri' => 1, 'message'=> '[error] Syslog test 2>1'), record)
      end

      text = '<1>Feb 28 12:00:02 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 12:00:02", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected.merge('pri' => 1), record)
      end

      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end
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

      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd 11111 ID24224 [exampleSDID@20224 iut="3" eventSource="Application" eventID="11211"] Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "11111", record["pid"]
        assert_equal "ID24224", record["msgid"]
        assert_equal "[exampleSDID@20224 iut=\"3\" eventSource=\"Application\" eventID=\"11211\"]",
                     record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end

      text = '<16>Feb 28 12:00:02 192.168.0.1 fluentd[11111]: [error] Syslog test'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("Feb 28 12:00:02", format: '%b %d %M:%S:%H'), time)
        assert_equal(@expected.merge('pri' => 16), record)
      end

      text = '<16>1 2017-02-06T13:14:15.003Z 192.168.0.1 fluentd - - - Hi, from Fluentd!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15.003Z", format: '%Y-%m-%dT%H:%M:%S.%L%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd!", record["message"]
      end

      text = '<16>1 2017-02-06T13:14:15Z 192.168.0.1 fluentd - - - Hi, from Fluentd without subseconds!'
      @parser.instance.parse(text) do |time, record|
        assert_equal(event_time("2017-02-06T13:14:15Z", format: '%Y-%m-%dT%H:%M:%S%z'), time)
        assert_equal "-", record["pid"]
        assert_equal "-", record["msgid"]
        assert_equal "-", record["extradata"]
        assert_equal "Hi, from Fluentd without subseconds!", record["message"]
      end
    end
  end

  # "parser_type" config shouldn't hide Fluent::Plugin::Parser#plugin_type
  # https://github.com/fluent/fluentd/issues/3296
  data('regexp' => :regexp, 'fast' => :string)
  def test_parser_type_method(engine)
    @parser.configure({'parser_type' => engine.to_s})
    assert_equal(:text_per_line, @parser.instance.parser_type)
  end

  data('regexp' => :regexp, 'string' => :string)
  def test_parser_engine(engine)
    @parser.configure({'parser_engine' => engine.to_s})
    assert_equal(engine, @parser.instance.parser_engine)
  end
end
