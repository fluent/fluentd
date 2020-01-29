require_relative '../../helper'
require 'fluent/plugin/in_tail/position_file'

require 'tempfile'

class IntailPositionFileTest < Test::Unit::TestCase
  setup do
    @file = Tempfile.new('intail_position_file_test')
  end

  teardown do
    @file.close rescue nil
    @file.unlink rescue nil
  end

  UNWATCHED_STR = '%016x' % Fluent::Plugin::TailInput::PositionFile::UNWATCHED_POSITION
  TEST_CONTENT = <<~EOF
    valid_path\t0000000000000002\t0000000000000001
    inode23bit\t0000000000000000\t00000000
    invalidpath100000000000000000000000000000000
    unwatched\t#{UNWATCHED_STR}\t0000000000000000
  EOF

  def write_data(f, content)
    f.write(content)
    f.seek(0)
  end

  sub_test_case '.compact' do
    test 'compact invalid and convert 32 bit inode value' do
      write_data(@file, TEST_CONTENT)
      Fluent::Plugin::TailInput::PositionFile.compact(@file)

      @file.seek(0)
      lines = @file.readlines
      assert_equal 2, lines.size
      assert_equal "valid_path\t0000000000000002\t0000000000000001\n", lines[0]
      assert_equal "inode23bit\t0000000000000000\t0000000000000000\n", lines[1]
    end

    test 'compact data if duplicated line' do
      write_data(@file, <<~EOF)
        valid_path\t0000000000000002\t0000000000000001
        valid_path\t0000000000000003\t0000000000000004
      EOF
      Fluent::Plugin::TailInput::PositionFile.compact(@file)

      @file.seek(0)
      lines = @file.readlines
      assert_equal "valid_path\t0000000000000003\t0000000000000004\n", lines[0]
    end
  end

  test '.parse' do
    write_data(@file, TEST_CONTENT)
    Fluent::Plugin::TailInput::PositionFile.parse(@file)

    @file.seek(0)
    lines = @file.readlines
    assert_equal 2, lines.size
    assert_equal "valid_path\t0000000000000002\t0000000000000001\n", lines[0]
    assert_equal "inode23bit\t0000000000000000\t0000000000000000\n", lines[1]
  end

  sub_test_case '#[]' do
    test 'return entry' do
      write_data(@file, TEST_CONTENT)
      pf = Fluent::Plugin::TailInput::PositionFile.parse(@file)

      f = pf['valid_path']
      assert_equal Fluent::Plugin::TailInput::FilePositionEntry, f.class
      assert_equal 2, f.read_pos
      assert_equal 1, f.read_inode

      @file.seek(0)
      lines = @file.readlines
      assert_equal 2, lines.size

      f = pf['nonexist_path']
      assert_equal Fluent::Plugin::TailInput::FilePositionEntry, f.class
      assert_equal 0, f.read_pos
      assert_equal 0, f.read_inode

      @file.seek(0)
      lines = @file.readlines
      assert_equal 3, lines.size
      assert_equal "nonexist_path\t0000000000000000\t0000000000000000\n", lines[2]
    end

    test 'does not change other value position if other entry try to write' do
      write_data(@file, TEST_CONTENT)
      pf = Fluent::Plugin::TailInput::PositionFile.parse(@file)

      f = pf['nonexist_path']
      assert_equal 0, f.read_inode
      assert_equal 0, f.read_pos

      pf['valid_path'].update(1, 2)

      f = pf['nonexist_path']
      assert_equal 0, f.read_inode
      assert_equal 0, f.read_pos

      pf['nonexist_path'].update(1, 2)
      assert_equal 1, f.read_inode
      assert_equal 2, f.read_pos
    end
  end

  sub_test_case '#unwatch' do
    test 'deletes entry by path' do
      write_data(@file, TEST_CONTENT)
      pf = Fluent::Plugin::TailInput::PositionFile.parse(@file)
      p1 = pf['valid_path']
      assert_equal Fluent::Plugin::TailInput::FilePositionEntry, p1.class

      pf.unwatch('valid_path')
      assert_equal p1.read_pos, Fluent::Plugin::TailInput::PositionFile::UNWATCHED_POSITION

      p2 = pf['valid_path']
      assert_equal Fluent::Plugin::TailInput::FilePositionEntry, p2.class

      assert_not_equal p1, p2
    end
  end

  sub_test_case 'FilePositionEntry' do
    FILE_POS_CONTENT = <<~EOF
      valid_path\t0000000000000002\t0000000000000001
      valid_path2\t0000000000000003\t0000000000000002
    EOF

    def build_files(file)
      r = {}

      file.each_line do |line|
        m = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.match(line)
        path = m[1]
        pos = m[2].to_i(16)
        ino = m[3].to_i(16)
        seek = file.pos - line.bytesize + path.bytesize + 1
        r[path] = Fluent::Plugin::TailInput::FilePositionEntry.new(@file, Mutex.new, seek, pos, ino)
      end

      r
    end

    test '#update' do
      write_data(@file, FILE_POS_CONTENT)
      fs = build_files(@file)
      f = fs['valid_path']
      f.update(11, 10)

      @file.seek(0)
      lines = @file.readlines
      assert_equal 2, lines.size
      assert_equal "valid_path\t000000000000000a\t000000000000000b\n", lines[0]
      assert_equal "valid_path2\t0000000000000003\t0000000000000002\n", lines[1]
    end

    test '#update_pos' do
      write_data(@file, FILE_POS_CONTENT)
      fs = build_files(@file)
      f = fs['valid_path']
      f.update_pos(10)

      @file.seek(0)
      lines = @file.readlines
      assert_equal 2, lines.size
      assert_equal "valid_path\t000000000000000a\t0000000000000001\n", lines[0]
      assert_equal "valid_path2\t0000000000000003\t0000000000000002\n", lines[1]
    end

    test '#read_pos' do
      write_data(@file, FILE_POS_CONTENT)
      fs = build_files(@file)
      f = fs['valid_path']
      assert_equal 2, f.read_pos

      f.update_pos(10)
      assert_equal 10, f.read_pos

      f.update(2, 11)
      assert_equal 11, f.read_pos
    end

    test '#read_inode' do
      write_data(@file, FILE_POS_CONTENT)
      fs = build_files(@file)
      f = fs['valid_path']
      assert_equal 1, f.read_inode
      f.update_pos(10)
      assert_equal 1, f.read_inode

      f.update(2, 11)
      assert_equal 2, f.read_inode
    end
  end
end
