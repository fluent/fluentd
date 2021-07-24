require_relative '../../helper'
require 'fluent/plugin/in_tail/position_file'
require 'fluent/plugin/in_tail'

require 'fileutils'
require 'tempfile'

class IntailPositionFileTest < Test::Unit::TestCase
  setup do
    @file = Tempfile.new('intail_position_file_test').binmode
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

  def follow_inodes_block
    [true, false].each do |follow_inodes|
      yield follow_inodes
    end
  end

  test '.load' do
    write_data(@file, TEST_CONTENT)
    Fluent::Plugin::TailInput::PositionFile.load(@file, false, {}, **{logger: $log})

    @file.seek(0)
    lines = @file.readlines
    assert_equal 2, lines.size
    assert_equal "valid_path\t0000000000000002\t0000000000000001\n", lines[0]
    assert_equal "inode23bit\t0000000000000000\t0000000000000000\n", lines[1]
  end

  sub_test_case '#try_compact' do
    test 'compact invalid and convert 32 bit inode value' do
      write_data(@file, TEST_CONTENT)
      Fluent::Plugin::TailInput::PositionFile.new(@file, false, **{logger: $log}).try_compact

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
      Fluent::Plugin::TailInput::PositionFile.new(@file, false, **{logger: $log}).try_compact

      @file.seek(0)
      lines = @file.readlines
      assert_equal "valid_path\t0000000000000003\t0000000000000004\n", lines[0]
    end

    test 'does not change when the file is changed' do
      write_data(@file, TEST_CONTENT)
      pf = Fluent::Plugin::TailInput::PositionFile.new(@file, false, **{logger: $log})

      mock.proxy(pf).fetch_compacted_entries do |r|
        @file.write("unwatched\t#{UNWATCHED_STR}\t0000000000000000\n")
        r
      end

      pf.try_compact

      @file.seek(0)
      lines = @file.readlines
      assert_equal 5, lines.size
    end

    test 'update seek position of remained position entry' do
      pf = Fluent::Plugin::TailInput::PositionFile.new(@file, false, **{logger: $log})
      target_info1 = Fluent::Plugin::TailInput::TargetInfo.new('path1', -1)
      target_info2 = Fluent::Plugin::TailInput::TargetInfo.new('path2', -1)
      target_info3 = Fluent::Plugin::TailInput::TargetInfo.new('path3', -1)
      pf[target_info1]
      pf[target_info2]
      pf[target_info3]

      target_info1_2 = Fluent::Plugin::TailInput::TargetInfo.new('path1', 1234)
      pf.unwatch(target_info1_2)

      pf.try_compact

      @file.seek(0)
      lines = @file.readlines
      assert_equal "path2\t0000000000000000\t0000000000000000\n", lines[0]
      assert_equal "path3\t0000000000000000\t0000000000000000\n", lines[1]
      assert_equal 2, lines.size

      target_info2_2 = Fluent::Plugin::TailInput::TargetInfo.new('path2', 1235)
      target_info3_2 = Fluent::Plugin::TailInput::TargetInfo.new('path3', 1236)
      pf.unwatch(target_info2_2)
      pf.unwatch(target_info3_2)
      @file.seek(0)
      lines = @file.readlines
      assert_equal "path2\t#{UNWATCHED_STR}\t0000000000000000\n", lines[0]
      assert_equal "path3\t#{UNWATCHED_STR}\t0000000000000000\n", lines[1]
      assert_equal 2, lines.size
    end

    test 'should ignore initial existing files on follow_inode' do
      write_data(@file, TEST_CONTENT)
      pos_file = Fluent::Plugin::TailInput::PositionFile.load(@file, true, {}, **{logger: $log})
      @file.seek(0)
      assert_equal([], @file.readlines)

      @file.seek(0)
      write_data(@file, TEST_CONTENT)
      pos_file.try_compact

      @file.seek(0)
      assert_equal([
                     "valid_path\t0000000000000002\t0000000000000001\n",
                     "inode23bit\t0000000000000000\t0000000000000000\n",
                   ],
                   @file.readlines)
    end
  end

  sub_test_case '#load' do
    test 'compact invalid and convert 32 bit inode value' do
      write_data(@file, TEST_CONTENT)
      Fluent::Plugin::TailInput::PositionFile.load(@file, false, {}, **{logger: $log})

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
      Fluent::Plugin::TailInput::PositionFile.new(@file, false, **{logger: $log}).load

      @file.seek(0)
      lines = @file.readlines
      assert_equal "valid_path\t0000000000000003\t0000000000000004\n", lines[0]
    end
  end

  sub_test_case '#[]' do
    test 'return entry' do
      write_data(@file, TEST_CONTENT)
      pf = Fluent::Plugin::TailInput::PositionFile.load(@file, false, {}, **{logger: $log})

      valid_target_info = Fluent::Plugin::TailInput::TargetInfo.new('valid_path', File.stat(@file).ino)
      f = pf[valid_target_info]
      assert_equal Fluent::Plugin::TailInput::FilePositionEntry, f.class
      assert_equal 2, f.read_pos
      assert_equal 1, f.read_inode

      @file.seek(0)
      lines = @file.readlines
      assert_equal 2, lines.size

      nonexistent_target_info = Fluent::Plugin::TailInput::TargetInfo.new('nonexist_path', -1)
      f = pf[nonexistent_target_info]
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
      pf = Fluent::Plugin::TailInput::PositionFile.load(@file, false, {}, logger: $log)

      f = pf[Fluent::Plugin::TailInput::TargetInfo.new('nonexist_path', -1)]
      assert_equal 0, f.read_inode
      assert_equal 0, f.read_pos

      pf[Fluent::Plugin::TailInput::TargetInfo.new('valid_path', File.stat(@file).ino)].update(1, 2)

      f = pf[Fluent::Plugin::TailInput::TargetInfo.new('nonexist_path', -1)]
      assert_equal 0, f.read_inode
      assert_equal 0, f.read_pos

      pf[Fluent::Plugin::TailInput::TargetInfo.new('nonexist_path', -1)].update(1, 2)
      assert_equal 1, f.read_inode
      assert_equal 2, f.read_pos
    end
  end

  sub_test_case '#unwatch' do
    test 'deletes entry by path' do
      write_data(@file, TEST_CONTENT)
      pf = Fluent::Plugin::TailInput::PositionFile.load(@file, false, {}, logger: $log)
      inode1 = File.stat(@file).ino
      target_info1 = Fluent::Plugin::TailInput::TargetInfo.new('valid_path', inode1)
      p1 = pf[target_info1]
      assert_equal Fluent::Plugin::TailInput::FilePositionEntry, p1.class

      pf.unwatch(target_info1)
      assert_equal p1.read_pos, Fluent::Plugin::TailInput::PositionFile::UNWATCHED_POSITION

      inode2 = File.stat(@file).ino
      target_info2 = Fluent::Plugin::TailInput::TargetInfo.new('valid_path', inode2)
      p2 = pf[target_info2]
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

  sub_test_case "TargetInfo equality rules" do
    sub_test_case "== operator" do
      def test_equal
        t1 = Fluent::Plugin::TailInput::TargetInfo.new("test", 1234)
        t2 = Fluent::Plugin::TailInput::TargetInfo.new("test", 1235)

        assert_equal t1, t2
      end

      def test_not_equal
        t1 = Fluent::Plugin::TailInput::TargetInfo.new("test", 1234)
        t2 = Fluent::Plugin::TailInput::TargetInfo.new("test2", 1234)

        assert_not_equal t1, t2
      end
    end

    sub_test_case "eql? method" do
      def test_eql?
        t1 = Fluent::Plugin::TailInput::TargetInfo.new("test", 1234)
        t2 = Fluent::Plugin::TailInput::TargetInfo.new("test", 5321)

        assert do
          t1.eql? t2
        end
      end

      def test_not_eql?
        t1 = Fluent::Plugin::TailInput::TargetInfo.new("test2", 1234)
        t2 = Fluent::Plugin::TailInput::TargetInfo.new("test3", 1234)

        assert do
          !t1.eql? t2
        end
      end
    end

    sub_test_case "hash" do
      def test_equal
        t1 = Fluent::Plugin::TailInput::TargetInfo.new("test", 1234)
        t2 = Fluent::Plugin::TailInput::TargetInfo.new("test", 7321)

        assert_equal t1.hash, t2.hash
      end

      def test_not_equal
        t1 = Fluent::Plugin::TailInput::TargetInfo.new("test", 1234)
        t2 = Fluent::Plugin::TailInput::TargetInfo.new("test2", 1234)

        assert_not_equal t1.hash, t2.hash
      end
    end
  end
end
