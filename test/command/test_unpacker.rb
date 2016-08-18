require_relative '../helper'

require 'oj'
require 'flexmock/test_unit'

require 'fluent/command/unpacker'
require 'fluent/event'

class TestFluentUnpacker < ::Test::Unit::TestCase
  module ::Command
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
      fu = FluentUnpacker.new(argv)

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
      fu = FluentUnpacker.new(argv)

      flexstub(::Command) do |command|
        command.should_receive(:const_get).once.and_return(::Command::Dummy)
        assert_nothing_raised do
          fu.call
        end
      end
    end
  end
end

class TestBaseCommand < ::Test::Unit::TestCase
  TMP_DIR = File.expand_path(File.dirname(__FILE__) + "/../tmp/command/unpacker#{ENV['TEST_ENV_NUMBER']}")

  FORMAT_MSGPACK_STREAM = ->(e){ e.to_msgpack_stream }

  def create_message_packed_file(path, times = [Time.now.to_i], records = [{ 'message' => 'dummy' }])
    es = Fluent::MultiEventStream.new(times, records)
    v = FORMAT_MSGPACK_STREAM.call(es)
    File.open("#{TMP_DIR}/#{path}", 'w') do |f|
      f.print(v)
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
    ENV['TZ'] = old
  end

  def capture_stdout
    out = StringIO.new
    $stdout = out
    yield
    out.string.force_encoding('utf-8')
  ensure
    $stdout = STDOUT
  end
end

class TestHead < TestBaseCommand
  sub_test_case 'initialize' do
    data(
      'file is not passed' => [],
      'file is not found' => %w(invalid_path.log)
    )
    test 'should fail if file is invalid' do |argv|
      assert_raise(SystemExit) do
        capture_stdout { Command::Head.new(argv) }
      end
    end

    test 'should succeed if a file is valid' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}"]
      create_message_packed_file(file_name)

      assert_nothing_raised do
        Command::Head.new(argv)
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
        create_message_packed_file(@file_name, [Time.parse(@t).to_i], [@record])
        head = Command::Head.new(argv)
        out = capture_stdout { head.call }
        assert_equal "2011-01-02T13:14:15+00:00\t#{TMP_DIR}/#{@file_name}\t#{Oj.dump(@record)}\n", out
      end
    end

    test 'should set the number of lines to display' do
      argv = ["#{TMP_DIR}/#{@file_name}", '-n', '1']

      timezone do
        create_message_packed_file(@file_name, [Time.parse(@t).to_i, Time.parse(@t).to_i], [@record, @record])
        head = Command::Head.new(argv)
        out = capture_stdout { head.call }
        assert_equal "2011-01-02T13:14:15+00:00\t#{TMP_DIR}/#{@file_name}\t#{Oj.dump(@record)}\n", out
      end
    end

    test 'should fail when the number of lines is invalid' do
      argv = ["#{TMP_DIR}/#{@file_name}", '-n', '0']

      create_message_packed_file(@file_name)
      assert_raise(SystemExit) do
        capture_stdout { Command::Head.new(argv) }
      end
    end

    test 'should output content of a file with json format' do
      argv = ["#{TMP_DIR}/#{@file_name}", '--format=json']

      timezone do
        create_message_packed_file(@file_name, [Time.parse(@t).to_i], [@record])
        head = Command::Head.new(argv)
        out = capture_stdout { head.call }
        assert_equal "#{Oj.dump(@record)}\n", out
      end
    end

    test 'should fail with an invalid format' do
      argv = ["#{TMP_DIR}/#{@file_name}", '--format=invalid']

      timezone do
        create_message_packed_file(@file_name, [Time.parse(@t).to_i], [@record])
        head = Command::Head.new(argv)

        assert_raise(SystemExit) do
          capture_stdout { head.call }
        end
      end
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
        capture_stdout { Command::Head.new(argv) }
      end
    end

    test 'should succeed if a file is valid' do
      file_name = 'packed.log'
      argv = ["#{TMP_DIR}/#{file_name}"]
      create_message_packed_file(file_name)

      assert_nothing_raised do
        Command::Cat.new(argv)
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
        create_message_packed_file(@file_name, [Time.parse(@t).to_i], [@record])
        head = Command::Cat.new(argv)
        out = capture_stdout { head.call }
        assert_equal "2011-01-02T13:14:15+00:00\t#{TMP_DIR}/#{@file_name}\t#{Oj.dump(@record)}\n", out
      end
    end

    test 'should output content of a file with json format' do
      argv = ["#{TMP_DIR}/#{@file_name}", '--format=json']

      timezone do
        create_message_packed_file(@file_name, [Time.parse(@t).to_i], [@record])
        head = Command::Cat.new(argv)
        out = capture_stdout { head.call }
        assert_equal "#{Oj.dump(@record)}\n", out
      end
    end

    test 'should fail with an invalid format' do
      argv = ["#{TMP_DIR}/#{@file_name}", '--format=invalid']

      timezone do
        create_message_packed_file(@file_name, [Time.parse(@t).to_i], [@record])
        head = Command::Cat.new(argv)

        assert_raise(SystemExit) do
          capture_stdout { head.call }
        end
      end
    end
  end
end

class TestFormats < TestBaseCommand
  test 'parse_option!' do
    assert_raise(SystemExit) do
      capture_stdout do
        Command::Formats.new(['--plugins=invalid_dir_path'])
      end
    end
  end

  sub_test_case 'call' do
    test 'display available plugins' do
      f = Command::Formats.new
      out = capture_stdout { f.call }
      assert out.include?('json')
      assert out.include?('csv')
    end

    test 'add new plugins using --plugins option' do
      dir_path = File.expand_path(File.dirname(__FILE__) + '/../scripts/fluent/plugin')

      f = Command::Formats.new(["--plugins=#{dir_path}"])
      out = capture_stdout { f.call }
      assert out.include?('json')
      assert out.include?('csv')
      assert out.include?('known')
    end
  end
end
