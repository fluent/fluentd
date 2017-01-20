require_relative '../helper'

require 'yajl'
require 'flexmock/test_unit'

require 'fluent/command/binlog_reader'
require 'fluent/event'

class TestFluentBinlogReader < ::Test::Unit::TestCase
  module ::BinlogReaderCommand
    class Dummy < Base
      def call; end
    end
  end

  def suppress_stdout
    out = StringIO.new
    $stdout = out
    yield
  ensure
    $stdout = STDOUT
  end

  sub_test_case 'call' do
    data(
      empty: [],
      invalid: %w(invalid packed.log),
    )
    test 'should fail when invalid command' do |argv|
      fu = FluentBinlogReader.new(argv)

      assert_raise(SystemExit) do
        suppress_stdout { fu.call }
      end
    end

    data(
      cat: %w(cat packed.log),
      head: %w(head packed.log),
      formats: %w(formats packed.log)
    )
    test 'should succeed when valid command' do |argv|
      fu = FluentBinlogReader.new(argv)

      flexstub(::BinlogReaderCommand) do |command|
        command.should_receive(:const_get).once.and_return(::BinlogReaderCommand::Dummy)
        assert_nothing_raised do
          fu.call
        end
      end
    end
  end
end

class TestBaseCommand < ::Test::Unit::TestCase
  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/command/binlog_reader#{ENV['TEST_ENV_NUMBER']}")

  def create_message_packed_file(path, times = [event_time], records = [{ 'message' => 'dummy' }])
    es = Fluent::MultiEventStream.new(times, records)
    v = es.to_msgpack_stream
    out_path = "#{TMP_DIR}/#{path}"
    File.open(out_path, 'wb') do |f|
      f.print(v)
    end
    waiting(5) do
      sleep 0.5 until File.size(out_path) == v.bytesize
    end
  end

  def setup
    FileUtils.rm_rf(TMP_DIR)
    FileUtils.mkdir_p(TMP_DIR)
  end

  def timezone(timezone = 'UTC')
    old = ENV['TZ']
    ENV['TZ'] = timezone
    yield
  ensure
    ENV['TZ'] = old
  end
end

class TestHead < TestBaseCommand
  sub_test_case 'initialize' do
    data(
      'file is not passed' => %w(),
      'file is not found' => %w(invalid_path.log)
    )
    test 'should fail if file is invalid' do |argv|
      assert_raise(SystemExit) do
        capture_stdout { BinlogReaderCommand::Head.new(argv) }
      end
    end

    test 'should succeed if a file is valid' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}"]
      create_message_packed_file(file_name)

      assert_nothing_raised do
        BinlogReaderCommand::Head.new(argv)
      end
    end

    test 'should fail when config_params format is invalid' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}", '--format=csv', '-e', 'only_key']
      create_message_packed_file(file_name)

      assert_raise(SystemExit) do
        capture_stdout { BinlogReaderCommand::Head.new(argv) }
      end
    end

    test 'should succeed if config_params format is valid' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}", '--format=csv', '-e', 'fields=message']
      create_message_packed_file(file_name)

      assert_nothing_raised do
        capture_stdout { BinlogReaderCommand::Head.new(argv) }
      end
    end
  end

  sub_test_case 'call' do
    setup do
      @file_name = 'packed.log'
      @t = '2011-01-02 13:14:15 UTC'
      @record = { 'message' => 'dummy' }
    end

    test 'should output the beginning of the file with default format (out_file)' do
      argv = ["#{TMP_DIR}/#{@file_name}"]

      timezone do
        create_message_packed_file(@file_name, [event_time(@t).to_i] * 6, [@record] * 6)
        head = BinlogReaderCommand::Head.new(argv)
        out = capture_stdout { head.call }
        assert_equal "2011-01-02T13:14:15+00:00\t#{TMP_DIR}/#{@file_name}\t#{Yajl.dump(@record)}\n" * 5, out
      end
    end

    test 'should set the number of lines to display' do
      argv = ["#{TMP_DIR}/#{@file_name}", '-n', '1']

      timezone do
        create_message_packed_file(@file_name, [event_time(@t).to_i] * 6, [@record] * 6)
        head = BinlogReaderCommand::Head.new(argv)
        out = capture_stdout { head.call }
        assert_equal "2011-01-02T13:14:15+00:00\t#{TMP_DIR}/#{@file_name}\t#{Yajl.dump(@record)}\n", out
      end
    end

    test 'should fail when the number of lines is invalid' do
      argv = ["#{TMP_DIR}/#{@file_name}", '-n', '0']

      create_message_packed_file(@file_name)
      assert_raise(SystemExit) do
        capture_stdout { BinlogReaderCommand::Head.new(argv) }
      end
    end

    test 'should output content of a file with json format' do
      argv = ["#{TMP_DIR}/#{@file_name}", '--format=json']

      timezone do
        create_message_packed_file(@file_name, [event_time(@t).to_i], [@record])
        head = BinlogReaderCommand::Head.new(argv)
        out = capture_stdout { head.call }
        assert_equal "#{Yajl.dump(@record)}\n", out
      end
    end

    test 'should fail with an invalid format' do
      argv = ["#{TMP_DIR}/#{@file_name}", '--format=invalid']

      timezone do
        create_message_packed_file(@file_name, [event_time(@t).to_i], [@record])
        head = BinlogReaderCommand::Head.new(argv)

        assert_raise(SystemExit) do
          capture_stdout { head.call }
        end
      end
    end

    test 'should succeed if multiple config_params format' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}", '--format=csv', '-e', 'fields=message,fo', '-e', 'delimiter=|']
      create_message_packed_file(file_name, [event_time], [{ 'message' => 'dummy', 'fo' => 'dummy2' }])

      head = BinlogReaderCommand::Head.new(argv)
      assert_equal "\"dummy\"|\"dummy2\"\n", capture_stdout { head.call }
    end
  end
end

class TestCat < TestBaseCommand
  sub_test_case 'initialize' do
    data(
      'file is not passed' => [],
      'file is not found' => %w(invalid_path.log)
    )
    test 'should fail if a file is invalid' do |argv|
      assert_raise(SystemExit) do
        capture_stdout { BinlogReaderCommand::Head.new(argv) }
      end
    end

    test 'should succeed if a file is valid' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}"]
      create_message_packed_file(file_name)

      assert_nothing_raised do
        BinlogReaderCommand::Cat.new(argv)
      end
    end

    test 'should fail when config_params format is invalid' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}", '--format=json', '-e', 'only_key']
      create_message_packed_file(file_name)

      assert_raise(SystemExit) do
        capture_stdout { BinlogReaderCommand::Cat.new(argv) }
      end
    end

    test 'should succeed when config_params format is valid' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}", '--format=csv', '-e', 'fields=message']
      create_message_packed_file(file_name)

      assert_nothing_raised do
        capture_stdout { BinlogReaderCommand::Cat.new(argv) }
      end
    end
  end

  sub_test_case 'call' do
    setup do
      @file_name = 'packed.log'
      @t = '2011-01-02 13:14:15 UTC'
      @record = { 'message' => 'dummy' }
    end

    test 'should output the file with default format(out_file)' do
      argv = ["#{TMP_DIR}/#{@file_name}"]

      timezone do
        create_message_packed_file(@file_name, [event_time(@t).to_i] * 6, [@record] * 6)
        head = BinlogReaderCommand::Cat.new(argv)
        out = capture_stdout { head.call }
        assert_equal "2011-01-02T13:14:15+00:00\t#{TMP_DIR}/#{@file_name}\t#{Yajl.dump(@record)}\n" * 6, out
      end
    end

    test 'should set the number of lines to display' do
      argv = ["#{TMP_DIR}/#{@file_name}", '-n', '1']

      timezone do
        create_message_packed_file(@file_name, [event_time(@t).to_i] * 6, [@record] * 6)
        head = BinlogReaderCommand::Cat.new(argv)
        out = capture_stdout { head.call }
        assert_equal "2011-01-02T13:14:15+00:00\t#{TMP_DIR}/#{@file_name}\t#{Yajl.dump(@record)}\n", out
      end
    end

    test 'should output content of a file with json format' do
      argv = ["#{TMP_DIR}/#{@file_name}", '--format=json']

      timezone do
        create_message_packed_file(@file_name, [event_time(@t).to_i], [@record])
        head = BinlogReaderCommand::Cat.new(argv)
        out = capture_stdout { head.call }
        assert_equal "#{Yajl.dump(@record)}\n", out
      end
    end

    test 'should fail with an invalid format' do
      argv = ["#{TMP_DIR}/#{@file_name}", '--format=invalid']

      timezone do
        create_message_packed_file(@file_name, [event_time(@t).to_i], [@record])
        head = BinlogReaderCommand::Cat.new(argv)

        assert_raise(SystemExit) do
          capture_stdout { head.call }
        end
      end
    end

    test 'should succeed if multiple config_params format' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}", '--format=csv', '-e', 'fields=message,fo', '-e', 'delimiter=|']
      create_message_packed_file(file_name, [event_time], [{ 'message' => 'dummy', 'fo' => 'dummy2' }])

      head = BinlogReaderCommand::Cat.new(argv)
      assert_equal "\"dummy\"|\"dummy2\"\n", capture_stdout { head.call }
    end
  end
end

class TestFormats < TestBaseCommand
  test 'parse_option!' do
    assert_raise(SystemExit) do
      capture_stdout do
        BinlogReaderCommand::Formats.new(['--plugin=invalid_dir_path'])
      end
    end
  end

  sub_test_case 'call' do
    test 'display available plugins' do
      f = BinlogReaderCommand::Formats.new
      out = capture_stdout { f.call }
      assert out.include?('json')
      assert out.include?('csv')
    end

    test 'add new plugins using --plugin option' do
      dir_path = File.expand_path(File.dirname(__FILE__) + '/../scripts/fluent/plugin/formatter1')

      f = BinlogReaderCommand::Formats.new(["--plugin=#{dir_path}"])
      out = capture_stdout { f.call }
      assert out.include?('json')
      assert out.include?('csv')
      assert out.include?('test1')
    end

    test 'add multiple plugins using --plugin option' do
      dir_path1 = File.expand_path(File.dirname(__FILE__) + '/../scripts/fluent/plugin/formatter1')
      dir_path2 = File.expand_path(File.dirname(__FILE__) + '/../scripts/fluent/plugin/formatter2')

      f = BinlogReaderCommand::Formats.new(["--plugin=#{dir_path1}", '-p', dir_path2])
      out = capture_stdout { f.call }
      assert out.include?('json')
      assert out.include?('csv')
      assert out.include?('test1')
      assert out.include?('test2')
    end
  end
end
