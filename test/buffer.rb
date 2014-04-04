require File.dirname(__FILE__) + '/helper'

require 'fluent/buffer'

class BasicBufferTest < Test::Unit::TestCase
  include Fluent

  def setup
    @buffer = BasicBuffer.new
  end

  def configure(str)
    config = Config.parse(str, '(test)')
    @buffer.configure(config)
  end

  def test_configure_default
    configure('')
    assert_equal :raise_exception, @buffer.buffer_queue_limit_log_level
  end

  def test_configure_with_valid_param
    configure(<<-EOC)
buffer_queue_limit_log_level info
    EOC
    assert_equal :info, @buffer.buffer_queue_limit_log_level
  end

  def test_configure_with_invalid_param
    assert_raise(ConfigError) do
      configure(<<-EOC)
buffer_queue_limit_log_level invalid
      EOC
    end
  end

  def test_log_or_raise_exception_queue_limit_with_raise_exception
    configure(<<-EOC)
buffer_queue_limit_log_level raise_exception
    EOC
    assert_raise(BufferQueueLimitError) do
      @buffer.send(:log_or_raise_exception_queue_limit)
    end
  end

  def test_log_or_raise_exception_queue_limit_with_info
    configure(<<-EOC)
buffer_queue_limit_log_level info
    EOC
    out = capture_log do
      assert_nothing_raised do
        @buffer.send(:log_or_raise_exception_queue_limit)
      end
    end
    pattern = /#{Regexp.escape('[info]: queue size exceeds limit')}$/
    assert_match pattern, out
  end

  def test_log_or_raise_exception_queue_limit_with_off
    configure(<<-EOC)
buffer_queue_limit_log_level off
    EOC
    out = capture_log do
      assert_nothing_raised do
        @buffer.send(:log_or_raise_exception_queue_limit)
      end
    end
    assert_equal '', out
  end

  private
  def capture_log(&block)
    tmp = $log
    string_io = StringIO.new
    $log = Fluent::Log.new(string_io)
    yield
    return string_io.string
  ensure
    $log = tmp
  end
end

