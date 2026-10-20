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
      pe = 'position_file'
      stub(pe).read_pos { 0 }
      stub(pe).update_pos { |val| update_pos = val }
      pe
    end

    returned_lines = ''
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

  test 'an asynchronous receive commits position only after successful completion' do
    text = "first\nsecond\n"
    @file.write(text)
    @file.close
    watcher = create_watcher
    position = 0
    stub(watcher).pe do
      pe = 'position_file'
      stub(pe).read_pos { position }
      stub(pe).update_pos { |value| position = value }
      pe
    end
    returned_lines = nil
    handler = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(
      watcher, path: @file.path, read_lines_limit: 1, read_bytes_limit_per_second: -1,
      log: $log, open_on_every_update: false, metrics: @metrics
    ) do |lines, _tail_watcher|
      returned_lines = lines.dup
      Fluent::Plugin::TailInput::TailWatcher::IOHandler::ASYNC_PENDING
    end

    handler.on_notify
    assert_true handler.pending?
    assert_equal 0, position
    assert_equal ["first\n", "second\n"], returned_lines
    assert_true handler.complete_async(true)
    assert_equal text.bytesize, position
    assert_false handler.pending?
  end

  test 'a batch rejected by a full queue resumes reading after asynchronous retry' do
    text = Array.new(2000) { |index| "#{index} #{'x' * 100}\n" }.join
    @file.write(text)
    @file.close
    watcher = create_watcher
    calls = 0
    received = []
    handler = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(
      watcher, path: @file.path, read_lines_limit: 1, read_bytes_limit_per_second: -1,
      log: $log, open_on_every_update: false, metrics: @metrics
    ) do |lines, _tail_watcher|
      calls += 1
      if calls == 1
        false
      else
        received.concat(lines)
        Fluent::Plugin::TailInput::TailWatcher::IOHandler::ASYNC_PENDING
      end
    end

    handler.on_notify
    assert_false handler.pending?
    handler.on_notify
    assert_true handler.pending?
    assert_true handler.complete_async(true)

    until handler.drained?
      handler.on_notify
      handler.complete_async(true) if handler.pending?
    end
    assert_equal text.lines, received
    assert_equal text.bytesize, watcher.pe.read_pos
  ensure
    handler&.close
  end

  test 'synchronous retry preserves the existing per-notification read behavior' do
    text = "#{'x' * 1000}\n" * 200
    @file.write(text)
    @file.close
    watcher = create_watcher
    calls = 0
    handler = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(
      watcher, path: @file.path, read_lines_limit: 1, read_bytes_limit_per_second: -1,
      log: $log, open_on_every_update: false, metrics: @metrics
    ) do
      calls += 1
      calls > 1
    end

    handler.on_notify
    assert_equal 0, watcher.pe.read_pos
    handler.on_notify
    assert_operator watcher.pe.read_pos, :>, 0
    assert_operator watcher.pe.read_pos, :<, text.bytesize
    assert_equal 2, calls
  ensure
    handler&.close
  end

  test 'a failed asynchronous position write clears pending state and retains the batch' do
    @file.write("line\n")
    @file.close
    watcher = create_watcher
    position_entry = watcher.pe
    writes = 0
    original_update = position_entry.method(:update_pos)
    position_entry.define_singleton_method(:update_pos) do |position|
      writes += 1
      raise Errno::ENOSPC if writes == 1
      original_update.call(position)
    end
    batches = []
    handler = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(
      watcher, path: @file.path, read_lines_limit: 1, read_bytes_limit_per_second: -1,
      log: $log, open_on_every_update: false, metrics: @metrics
    ) do |lines, _tail_watcher|
      batches << lines.dup
      Fluent::Plugin::TailInput::TailWatcher::IOHandler::ASYNC_PENDING
    end

    handler.on_notify
    assert_nothing_raised { handler.complete_async(true) }
    assert_false handler.pending?
    assert_equal 0, position_entry.read_pos
    handler.on_notify
    assert_true handler.pending?
    handler.complete_async(true)
    assert_false handler.pending?
    assert_equal 5, position_entry.read_pos
    assert_equal [["line\n"], ["line\n"]], batches
  ensure
    handler&.close
  end

  test 'a final force-close releases a file even while its batch is pending' do
    @file.write("line\n")
    @file.close
    watcher = create_watcher
    handler = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(
      watcher, path: @file.path, read_lines_limit: 1, read_bytes_limit_per_second: -1,
      log: $log, open_on_every_update: false, metrics: @metrics
    ) do
      Fluent::Plugin::TailInput::TailWatcher::IOHandler::ASYNC_PENDING
    end
    watcher.instance_variable_set(:@io_handler, handler)
    handler.on_notify
    watcher.close
    assert_true handler.opened?

    watcher.force_close
    assert_false handler.opened?
    assert_false watcher.pending?
  ensure
    handler&.close
  end

  test 'a null handler is drained without pending work' do
    handler = Fluent::Plugin::TailInput::TailWatcher::NullIOHandler.new
    assert_true handler.drained?
    assert_false handler.pending?
  end

  test 'a failed asynchronous receive keeps the batch and position for retry' do
    text = "line\n"
    @file.write(text)
    @file.close
    watcher = create_watcher
    position = 0
    stub(watcher).pe do
      pe = 'position_file'
      stub(pe).read_pos { position }
      stub(pe).update_pos { |value| position = value }
      pe
    end
    calls = 0
    handler = Fluent::Plugin::TailInput::TailWatcher::IOHandler.new(
      watcher, path: @file.path, read_lines_limit: 10, read_bytes_limit_per_second: -1,
      log: $log, open_on_every_update: false, metrics: @metrics
    ) do
      calls += 1
      calls == 1 ? Fluent::Plugin::TailInput::TailWatcher::IOHandler::ASYNC_PENDING : true
    end

    handler.on_notify
    handler.on_notify
    assert_true handler.complete_async(false)
    assert_equal 0, position
    assert_false handler.pending?
    handler.on_notify

    assert_equal 2, calls
    assert_equal text.bytesize, position
  end

  sub_test_case 'when open_on_every_update is true and read_pos returns always 0' do
    test 'open new IO and change pos to 0 and read it' do
      text = "this line is test\ntest line is test\n"
      @file.write(text)
      @file.close

      update_pos = 0

      watcher = create_watcher
      stub(watcher).pe do
        pe = 'position_file'
        stub(pe).read_pos { 0 }
        stub(pe).update_pos { |val| update_pos = val }
        pe
      end

      returned_lines = ''
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
    test 'call receive_lines once when short line(less than 65536)' do
      text = "line\n" * 8
      @file.write(text)
      @file.close

      update_pos = 0

      watcher = create_watcher
      stub(watcher).pe do
        pe = 'position_file'
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

    test 'call receive_lines some times when long line(more than 65536)' do
      t = 'line' * (65536 / 8)
      text = "#{t}\n" * 8
      @file.write(text)
      @file.close

      update_pos = 0

      watcher = create_watcher
      stub(watcher).pe do
        pe = 'position_file'
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
        pe = 'position_file'
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
        pe = 'position_file'
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
        pe = 'position_file'
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
