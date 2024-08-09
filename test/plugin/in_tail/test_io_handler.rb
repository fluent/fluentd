# frozen_string_literal: true

require_relative '../../helper'

require 'fluent/plugin/in_tail'
require 'fluent/plugin/metrics_local'
require 'tempfile'

class IntailIOHandlerTest < Test::Unit::TestCase
  def setup
    Tempfile.create('intail_io_handler') do |file|
      file.binmode
      @file = file
      opened_file_metrics = Fluent::Plugin::LocalMetrics.new
      opened_file_metrics.configure(config_element('metrics', '', {}))
      closed_file_metrics = Fluent::Plugin::LocalMetrics.new
      closed_file_metrics.configure(config_element('metrics', '', {}))
      rotated_file_metrics = Fluent::Plugin::LocalMetrics.new
      rotated_file_metrics.configure(config_element('metrics', '', {}))
      throttling_metrics = Fluent::Plugin::LocalMetrics.new
      throttling_metrics.configure(config_element('metrics', '', {}))
      @metrics = Fluent::Plugin::TailInput::MetricsInfo.new(opened_file_metrics, closed_file_metrics, rotated_file_metrics, throttling_metrics)
      yield
    end
  end

  def create_target_info
    Fluent::Plugin::TailInput::TargetInfo.new(@file.path, Fluent::FileWrapper.stat(@file.path).ino)
  end

  def create_watcher
    Fluent::Plugin::TailInput::TailWatcher.new(create_target_info, nil, nil, nil, nil, nil, nil, nil, nil)
  end

  test '#on_notify load file content and passed it to receive_lines method' do
    text = "this line is test\ntest line is test\n"
    @file.write(text)
    @file.close

    watcher = create_watcher

    update_pos = 0

    stub(watcher).pe do
      pe = +'position_file'
      stub(pe).read_pos { 0 }
      stub(pe).update_pos { |val| update_pos = val }
      pe
    end

    returned_lines = +''
    r = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(watcher, path: @file.path, read_lines_limit: 100, read_bytes_limit_per_second: -1, log: $log, open_on_every_update: false, metrics: @metrics) do |lines, _watcher|
      returned_lines << lines.join
      true
    end

    r.on_notify
    assert_equal text.bytesize, update_pos
    assert_equal text, returned_lines

    r.on_notify

    assert_equal text.bytesize, update_pos
    assert_equal text, returned_lines
  end

  sub_test_case 'when open_on_every_update is true and read_pos returns always 0' do
    test 'open new IO and change pos to 0 and read it' do
      text = "this line is test\ntest line is test\n"
      @file.write(text)
      @file.close

      update_pos = 0

      watcher = create_watcher
      stub(watcher).pe do
        pe = +'position_file'
        stub(pe).read_pos { 0 }
        stub(pe).update_pos { |val| update_pos = val }
        pe
      end

      returned_lines = +''
      r = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(watcher, path: @file.path, read_lines_limit: 100, read_bytes_limit_per_second: -1, log: $log, open_on_every_update: true, metrics: @metrics) do |lines, _watcher|
        returned_lines << lines.join
        true
      end

      r.on_notify
      assert_equal text.bytesize, update_pos
      assert_equal text, returned_lines

      r.on_notify
      assert_equal text * 2, returned_lines
    end
  end

  sub_test_case 'when limit is 5' do
    test 'call receive_lines once when short line(less than 8192)' do
      text = "line\n" * 8
      @file.write(text)
      @file.close

      update_pos = 0

      watcher = create_watcher
      stub(watcher).pe do
        pe = +'position_file'
        stub(pe).read_pos { 0 }
        stub(pe).update_pos { |val| update_pos = val }
        pe
      end

      returned_lines = []
      r = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(watcher, path: @file.path, read_lines_limit: 5, read_bytes_limit_per_second: -1, log: $log, open_on_every_update: false, metrics: @metrics) do |lines, _watcher|
        returned_lines << lines.dup
        true
      end

      r.on_notify
      assert_equal text.bytesize, update_pos
      assert_equal 8, returned_lines[0].size
    end

    test 'call receive_lines some times when long line(more than 8192)' do
      t = 'line' * (8192 / 8)
      text = "#{t}\n" * 8
      @file.write(text)
      @file.close

      update_pos = 0

      watcher = create_watcher
      stub(watcher).pe do
        pe = +'position_file'
        stub(pe).read_pos { 0 }
        stub(pe).update_pos { |val| update_pos = val }
        pe
      end

      returned_lines = []
      r = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(watcher, path: @file.path, read_lines_limit: 5, read_bytes_limit_per_second: -1, log: $log, open_on_every_update: false, metrics: @metrics) do |lines, _watcher|
        returned_lines << lines.dup
        true
      end

      r.on_notify
      assert_equal text.bytesize, update_pos
      assert_equal 5, returned_lines[0].size
      assert_equal 3, returned_lines[1].size
    end
  end

  sub_test_case 'max_line_size' do
    test 'does not call receive_lines when line_size exceeds max_line_size' do
      t = 'x' * (8192)
      text = "#{t}\n"

      max_line_size = 8192

      @file.write(text)
      @file.close

      update_pos = 0

      watcher = create_watcher
      stub(watcher).pe do
        pe = +'position_file'
        stub(pe).read_pos {0}
        stub(pe).update_pos { |val| update_pos = val }
        pe
      end

      returned_lines = []
      r = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(watcher, path: @file.path, read_lines_limit: 1000, read_bytes_limit_per_second: -1, max_line_size: max_line_size, log: $log, open_on_every_update: false, metrics: @metrics) do |lines, _watcher|
        returned_lines << lines.dup
        true
      end

      r.on_notify
      assert_equal text.bytesize, update_pos
      assert_equal 0, returned_lines.size
    end

    data(
      "open_on_every_update false" => false,
      "open_on_every_update true" => true,
    )
    test 'manage pos correctly if a long line not having EOL occurs' do |open_on_every_update|
      max_line_size = 20
      returned_lines = []
      pos = 0

      watcher = create_watcher
      stub(watcher).pe do
        pe = +'position_file'
        stub(pe).read_pos { pos }
        stub(pe).update_pos { |val| pos = val }
        pe
      end

      io_handler = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(
        watcher, path: @file.path, read_lines_limit: 1000, read_bytes_limit_per_second: -1,
        max_line_size: max_line_size, log: $log, open_on_every_update: open_on_every_update,
        metrics: @metrics
      ) do |lines, _watcher|
        returned_lines << lines.dup
        true
      end

      short_line = "short line\n"
      long_lines = [
        "long line still not having EOL",
        " end of the line\n",
      ]

      @file.write(short_line)
      @file.write(long_lines[0])
      @file.flush
      io_handler.on_notify

      assert_equal [[short_line]], returned_lines
      assert_equal short_line.bytesize, pos

      @file.write(long_lines[1])
      @file.flush
      io_handler.on_notify

      assert_equal [[short_line]], returned_lines
      expected_size = short_line.bytesize + long_lines[0..1].map{|l| l.bytesize}.sum
      assert_equal expected_size, pos

      io_handler.close
    end

    data(
      "open_on_every_update false" => false,
      "open_on_every_update true" => true,
    )
    test 'discards a subsequent data in a long line even if restarting occurs between' do |open_on_every_update|
      max_line_size = 20
      returned_lines = []
      pos = 0

      watcher = create_watcher
      stub(watcher).pe do
        pe = +'position_file'
        stub(pe).read_pos { pos }
        stub(pe).update_pos { |val| pos = val }
        pe
      end

      io_handler = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(
        watcher, path: @file.path, read_lines_limit: 1000, read_bytes_limit_per_second: -1,
        max_line_size: max_line_size, log: $log, open_on_every_update: open_on_every_update,
        metrics: @metrics
      ) do |lines, _watcher|
        returned_lines << lines.dup
        true
      end

      short_line = "short line\n"
      long_lines = [
        "long line still not having EOL",
        " end of the line\n",
      ]

      @file.write(short_line)
      @file.write(long_lines[0])
      @file.flush
      io_handler.on_notify

      assert_equal [[short_line]], returned_lines

      io_handler.close
      io_handler = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(
        watcher, path: @file.path, read_lines_limit: 1000, read_bytes_limit_per_second: -1,
        max_line_size: max_line_size, log: $log, open_on_every_update: open_on_every_update,
        metrics: @metrics
      ) do |lines, _watcher|
        returned_lines << lines.dup
        true
      end

      @file.write(long_lines[1])
      @file.flush
      io_handler.on_notify

      assert_equal [[short_line]], returned_lines

      io_handler.close
    end
  end
end
