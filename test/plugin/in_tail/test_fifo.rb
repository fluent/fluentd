require_relative '../../helper'

require 'fluent/plugin/in_tail'

class IntailFIFO < Test::Unit::TestCase
  sub_test_case '#read_line' do
    test 'returns lines spliting per `\n`' do
      fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::ASCII_8BIT, Encoding::ASCII_8BIT, $log)
      text = ("test\n" * 3).force_encoding(Encoding::ASCII_8BIT)
      fifo << text
      lines = []
      fifo.read_lines(lines)
      assert_equal Encoding::ASCII_8BIT, lines[0].encoding
      assert_equal ["test\n", "test\n", "test\n"], lines
    end

    test 'concant line when line is separated' do
      fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::ASCII_8BIT, Encoding::ASCII_8BIT, $log)
      text = ("test\n" * 3 + 'test').force_encoding(Encoding::ASCII_8BIT)
      fifo << text
      lines = []
      fifo.read_lines(lines)
      assert_equal Encoding::ASCII_8BIT, lines[0].encoding
      assert_equal ["test\n", "test\n", "test\n"], lines

      fifo << "2\n"
      fifo.read_lines(lines)
      assert_equal Encoding::ASCII_8BIT, lines[0].encoding
      assert_equal ["test\n", "test\n", "test\n", "test2\n"], lines
    end

    test 'returns lines which convert encoding' do
      fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::ASCII_8BIT, Encoding::UTF_8, $log)
      text = ("test\n" * 3).force_encoding(Encoding::ASCII_8BIT)
      fifo << text
      lines = []
      fifo.read_lines(lines)
      assert_equal Encoding::UTF_8, lines[0].encoding
      assert_equal ["test\n", "test\n", "test\n"], lines
    end

    test 'reads lines as from_encoding' do
      fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::UTF_8, Encoding::ASCII_8BIT, $log)
      text = ("test\n" * 3).force_encoding(Encoding::UTF_8)
      fifo << text
      lines = []
      fifo.read_lines(lines)
      assert_equal Encoding::ASCII_8BIT, lines[0].encoding
      assert_equal ["test\n", "test\n", "test\n"], lines
    end

    sub_test_case 'when it includes multi byte chars' do
      test 'handles it as ascii_8bit' do
        fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::ASCII_8BIT, Encoding::ASCII_8BIT, $log)
        text = ("てすと\n" * 3).force_encoding(Encoding::ASCII_8BIT)
        fifo << text
        lines = []
        fifo.read_lines(lines)
        assert_equal Encoding::ASCII_8BIT, lines[0].encoding
        assert_equal ["てすと\n", "てすと\n", "てすと\n"].map { |e| e.force_encoding(Encoding::ASCII_8BIT) }, lines
      end

      test 'replaces character with ? when convert error happens' do
        fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::UTF_8, Encoding::ASCII_8BIT, $log)
        text = ("てすと\n" * 3).force_encoding(Encoding::UTF_8)
        fifo << text
        lines = []
        fifo.read_lines(lines)
        assert_equal Encoding::ASCII_8BIT, lines[0].encoding
        assert_equal ["???\n", "???\n", "???\n"].map { |e| e.force_encoding(Encoding::ASCII_8BIT) }, lines
      end
    end

    test 'reutrns nothing when buffer is empty' do
      fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::ASCII_8BIT, Encoding::ASCII_8BIT, $log)
      lines = []
      fifo.read_lines(lines)
      assert_equal [], lines

      text = "test\n" * 3
      fifo << text
      fifo.read_lines(lines)
      assert_equal ["test\n", "test\n", "test\n"], lines

      lines = []
      fifo.read_lines(lines)
      assert_equal [], lines
    end

    data('bigger than max_line_size', [
      ["test test test\n" * 3],
      [],
    ])
    data('less than or equal to max_line_size', [
      ["test\n" * 2],
      ["test\n", "test\n"],
    ])
    data('mix', [
      ["test test test\ntest\ntest test test\ntest\ntest test test\n"],
      ["test\n", "test\n"],
    ])
    data('mix and multiple', [
      [
        "test test test\ntest\n",
        "test",
        " test test\nt",
        "est\nt"
      ],
      ["test\n", "test\n"],
    ])
    data('remaining data bigger than max_line_size should be discarded', [
      [
        "test\nlong line still not having EOL",
        "following texts to the previous long line\ntest\n",
      ],
      ["test\n", "test\n"],
    ])
    test 'return lines only that size is less than or equal to max_line_size' do |(input_texts, expected)|
      max_line_size = 5
      fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::ASCII_8BIT, Encoding::ASCII_8BIT, $log, max_line_size)
      lines = []

      input_texts.each do |text|
        fifo << text.force_encoding(Encoding::ASCII_8BIT)
        fifo.read_lines(lines)
        # The size of remaining buffer (i.e. a line still not having EOL) must not exceed max_line_size.
        assert { fifo.buffer.bytesize <= max_line_size }
      end

      assert_equal expected, lines
    end
  end

  sub_test_case '#<<' do
    test 'does not make any change about encoding to an argument' do
      fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::ASCII_8BIT, Encoding::ASCII_8BIT, $log)
      text = ("test\n" * 3).force_encoding(Encoding::UTF_8)

      assert_equal Encoding::UTF_8, text.encoding
      fifo << text
      assert_equal Encoding::UTF_8, text.encoding
    end
  end

  sub_test_case '#reading_bytesize' do
    test 'returns buffer size' do
      fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::ASCII_8BIT, Encoding::ASCII_8BIT, $log)
      text = "test\n" * 3 + 'test'
      fifo << text

      assert_equal text.bytesize, fifo.reading_bytesize
      lines = []
      fifo.read_lines(lines)
      assert_equal ["test\n", "test\n", "test\n"], lines

      assert_equal 'test'.bytesize, fifo.reading_bytesize
      fifo << "2\n"
      fifo.read_lines(lines)
      assert_equal ["test\n", "test\n", "test\n", "test2\n"], lines

      assert_equal 0, fifo.reading_bytesize
    end

    test 'returns the entire line size even if the size is over max_line_size' do
      max_line_size = 20
      fifo = Fluent::Plugin::TailInput::TailWatcher::FIFO.new(Encoding::ASCII_8BIT, Encoding::ASCII_8BIT, $log, max_line_size)
      lines = []

      text = "long line still not having EOL"
      fifo << text
      fifo.read_lines(lines)
      assert_equal [], lines
      assert_equal 0, fifo.buffer.bytesize
      assert_equal text.bytesize, fifo.reading_bytesize

      text2 = " following texts"
      fifo << text2
      fifo.read_lines(lines)
      assert_equal [], lines
      assert_equal 0, fifo.buffer.bytesize
      assert_equal text.bytesize + text2.bytesize, fifo.reading_bytesize

      text3 = " end of the line\n"
      fifo << text3
      fifo.read_lines(lines)
      assert_equal [], lines
      assert_equal 0, fifo.buffer.bytesize
      assert_equal 0, fifo.reading_bytesize
    end
  end
end
