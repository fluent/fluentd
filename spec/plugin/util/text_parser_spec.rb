require 'fluentd/worker_global_methods'
require 'fluentd/plugin/util/text_parser'
require 'time'

describe Fluentd::Plugin::Util::TextParser do
  context Fluentd::Plugin::Util::TextParser::RegexpParser do
    str_time = '28/Feb/2013:12:00:00 +0900'
    expected_time = Time.strptime(str_time, '%d/%b/%Y:%H:%M:%S %z').to_i

    before :each do
      @parser = Fluentd::Plugin::Util::TextParser::RegexpParser.new(/^\[(?<time>[^\]]*)\] (?<message>.*)$/)
      @parser.init_configurable
      @parser.configure({})
    end

    it 'can parse log' do
      @parser.configure({'time_format' => "%d/%b/%Y:%H:%M:%S %z"})
      time, record = @parser.call("[#{str_time}] foobar")

      expect(time).to eq expected_time
      expect(record).to eq({'message' => 'foobar'})
    end

    it 'can parse log without time' do
      @parser = Fluentd::Plugin::Util::TextParser::RegexpParser.new(/^(?<message>.*)$/)
      @parser.init_configurable
      @parser.configure({})
      time, record = @parser.call('foobar')

      expect(time).not_to be_nil
      expect(record).to eq({'message' => 'foobar'})
    end
  end

  context Fluentd::Plugin::Util::TextParser::ApacheParser do
    str_time = '28/Feb/2013:12:00:00 +0900'
    expected_time = Time.strptime(str_time, '%d/%b/%Y:%H:%M:%S %z').to_i

    before :each do
      @parser = Fluentd::Plugin::Util::TextParser::ApacheParser.new
      @parser.init_configurable
      @parser.configure({})
    end

    it 'can parses apache log' do
      time, record = @parser.call(%Q{192.168.0.1 - - [#{str_time}] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"})
      expect(time).to eq expected_time
      expect(record).to eq({
                            'user'    => nil,
                            'method'  => 'GET',
                            'code'    => 200,
                            'size'    => 777,
                            'host'    => '192.168.0.1',
                            'path'    => '/',
                            'referer' => nil,
                            'agent'   => 'Opera/12.0'
                          })
    end
  end

  context Fluentd::Plugin::Util::TextParser::SyslogParser do
    str_time = 'Feb 28 12:00:00'
    expected_time = Time.strptime(str_time, '%b %d %H:%M:%S').to_i

    before :each do
      @parser = Fluentd::Plugin::Util::TextParser::SyslogParser.new
      @parser.init_configurable
      @parser.configure({})
    end

    it "can parses syslog" do
      time, record = @parser.call("#{str_time} 192.168.0.1 fluentd[11111]: [error] Syslog test")
      expect(time).to eq expected_time
      expect(record).to eq({
                            'host'    => '192.168.0.1',
                            'ident'   => 'fluentd',
                            'pid'     => '11111',
                            'message' => '[error] Syslog test'
                          })
    end
  end

  context Fluentd::Plugin::Util::TextParser::JSONParser do
    str_time = '2013-02-28 12:00:00 +0900'
    expected_time = Time.parse(str_time).to_i

    before :each do
      @parser = Fluentd::Plugin::Util::TextParser::JSONParser.new
      @parser.init_configurable
      @parser.configure({})
    end

    it "can parse json" do
      time, record = @parser.call('{"time":1362020400,"host":"192.168.0.1","size":777,"method":"PUT"}')
      expect(time).to eq expected_time
      expect(record).to eq({
                            'host'   => '192.168.0.1',
                            'size'   => 777,
                            'method' => 'PUT',
                          })
    end

    it "can parse json without time" do
      time, record = @parser.call('{"host":"192.168.0.1","size":777,"method":"PUT"}')
      expect(time).not_to be_nil
      expect(record).to eq({
                            'host'   => '192.168.0.1',
                            'size'   => 777,
                            'method' => 'PUT',
                          })
    end
  end

  context Fluentd::Plugin::Util::TextParser::LabeledTSVParser do
    str_time = '2013/02/28 12:00:00'
    expected_time = Time.strptime(str_time, '%Y/%m/%d %H:%M:%S').to_i

    before :each do
      @parser = Fluentd::Plugin::Util::TextParser::LabeledTSVParser.new
      @parser.init_configurable
      @parser.configure({})
    end

    it "have default delimiters" do
      expect(@parser.delimiter).to eq "\t"
      expect(@parser.label_delimiter).to eq ":"
    end

    it "can parse ltsv" do
      time, record = @parser.call("time:#{str_time}\thost:192.168.0.1\treq_id:111")
      expect(time).to eq expected_time
      expect(record).to eq({
                            'host'   => '192.168.0.1',
                            'req_id' => '111',
                          })
    end

    it "can parse ltsv without time" do
      time, record = @parser.call("host:192.168.0.1\treq_id:111")
      expect(time).not_to be_nil
      expect(record).to eq({
                            'host'   => '192.168.0.1',
                            'req_id' => '111',
                          })
    end

    it "can parse ltsv with customized delimiter" do
      @parser.configure(
        'delimiter'       => ',',
        'label_delimiter' => '=',
      )
      time, record = @parser.call("time=#{str_time},host=192.168.0.1,req_id=111")

      expect(time).to eq expected_time
      expect(record).to eq({
                            'host'   => '192.168.0.1',
                            'req_id' => '111',
                          })
    end

    it "can parse ltsv with customized time format" do
      str_time = '28/Feb/2013:12:00:00 +0900'
      time_format =  '%d/%b/%Y:%H:%M:%S %z'
      expected_time = Time.strptime(str_time, time_format).to_i

      @parser.configure(
        'time_key'    => 'mytime',
        'time_format' => time_format,
      )

      time, record = @parser.call("mytime:#{str_time}\thost:192.168.0.1\treq_id:111")

      expect(time).to eq expected_time
      expect(record).to eq({
                            'host'   => '192.168.0.1',
                            'req_id' => '111',
                          })
    end
  end

  context Fluentd::Plugin::Util::TextParser::NginxParser do
    str_time = '28/Feb/2013:12:00:00 +0900'
    expected_time = Time.strptime(str_time, '%d/%b/%Y:%H:%M:%S %z').to_i
    expected = {
      'remote'  => '127.0.0.1',
      'host'    => '192.168.0.1',
      'user'    => '-',
      'method'  => 'GET',
      'path'    => '/',
      'code'    => '200',
      'size'    => '777',
      'referer' => '-',
      'agent'   => 'Opera/12.0'
    }

    before :each do
      @parser = Fluentd::Plugin::Util::TextParser::TEMPLATE_FACTORIES['nginx'].call
      @parser.init_configurable
      @parser.configure({})
    end

    it "can parses nginx log" do
      time, record = @parser.call(%Q{127.0.0.1 192.168.0.1 - [#{str_time}] "GET / HTTP/1.1" 200 777 "-" "Opera/12.0"})
      expect(time).to eq expected_time
      expect(record).to eq(expected)
    end

    it "can parses nginx with empty included path" do
      time, record = @parser.call(%Q{127.0.0.1 192.168.0.1 - [#{str_time}] "GET /a[ ]b HTTP/1.1" 200 777 "-" "Opera/12.0"})
      expect(time).to eq expected_time
      expect(record).to eq(expected.merge('path' => '/a[ ]b'))
    end
  end
end
