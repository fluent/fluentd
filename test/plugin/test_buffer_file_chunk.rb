require_relative '../helper'
require 'fluent/plugin/buffer/file_chunk'
require 'fluent/plugin/compressable'
require 'fluent/unique_id'

require 'fileutils'
require 'msgpack'
require 'time'
require 'timecop'

class BufferFileChunkTest < Test::Unit::TestCase
  include Fluent::Plugin::Compressable

  setup do
    @klass = Fluent::Plugin::Buffer::FileChunk
    @chunkdir = File.expand_path('../../tmp/buffer_file_chunk', __FILE__)
    FileUtils.rm_r @chunkdir rescue nil
    FileUtils.mkdir_p @chunkdir
  end
  teardown do
    Timecop.return
  end

  Metadata = Struct.new(:timekey, :tag, :variables)
  def gen_metadata(timekey: nil, tag: nil, variables: nil)
    Metadata.new(timekey, tag, variables)
  end

  def read_metadata_file(path)
    File.open(path, 'rb'){|f| MessagePack.unpack(f.read, symbolize_keys: true) }
  end

  def gen_path(path)
    File.join(@chunkdir, path)
  end

  def gen_test_chunk_id
    require 'time'
    now = Time.parse('2016-04-07 14:31:33 +0900')
    u1 = ((now.to_i * 1000 * 1000 + now.usec) << 12 | 1725) # 1725 is one of `rand(0xfff)`
    u3 = 2979763054 # one of rand(0xffffffff)
    u4 = 438020492  # ditto
    [u1 >> 32, u1 & 0xffffffff, u3, u4].pack('NNNN')
    # unique_id.unpack('N*').map{|n| n.to_s(16)}.join => "52fde6425d7406bdb19b936e1a1ba98c"
  end

  def hex_id(id)
    id.unpack('N*').map{|n| n.to_s(16)}.join
  end

  sub_test_case 'classmethods' do
    data(
      correct_staged: ['/mydir/mypath/myfile.b00ff.log', :staged],
      correct_queued: ['/mydir/mypath/myfile.q00ff.log', :queued],
      incorrect_staged: ['/mydir/mypath/myfile.b00ff.log/unknown', :unknown],
      incorrect_queued: ['/mydir/mypath/myfile.q00ff.log/unknown', :unknown],
      output_file: ['/mydir/mypath/myfile.20160716.log', :unknown],
    )
    test 'can .assume_chunk_state' do |data|
      path, expected = data
      assert_equal expected, @klass.assume_chunk_state(path)
    end

    test '.generate_stage_chunk_path generates path with staged mark & chunk unique_id' do
      assert_equal gen_path("mychunk.b52fde6425d7406bdb19b936e1a1ba98c.log"), @klass.generate_stage_chunk_path(gen_path("mychunk.*.log"), gen_test_chunk_id)
      assert_raise RuntimeError.new("BUG: buffer chunk path on stage MUST have '.*.'") do
        @klass.generate_stage_chunk_path(gen_path("mychunk.log"), gen_test_chunk_id)
      end
      assert_raise RuntimeError.new("BUG: buffer chunk path on stage MUST have '.*.'") do
        @klass.generate_stage_chunk_path(gen_path("mychunk.*"), gen_test_chunk_id)
      end
      assert_raise RuntimeError.new("BUG: buffer chunk path on stage MUST have '.*.'") do
        @klass.generate_stage_chunk_path(gen_path("*.log"), gen_test_chunk_id)
      end
    end

    test '.generate_queued_chunk_path generates path with enqueued mark for staged chunk path' do
      assert_equal(
        gen_path("mychunk.q52fde6425d7406bdb19b936e1a1ba98c.log"),
        @klass.generate_queued_chunk_path(gen_path("mychunk.b52fde6425d7406bdb19b936e1a1ba98c.log"), gen_test_chunk_id)
      )
    end

    test '.generate_queued_chunk_path generates special path with chunk unique_id for non staged chunk path' do
      assert_equal(
        gen_path("mychunk.log.q52fde6425d7406bdb19b936e1a1ba98c.chunk"),
        @klass.generate_queued_chunk_path(gen_path("mychunk.log"), gen_test_chunk_id)
      )
      assert_equal(
        gen_path("mychunk.q55555555555555555555555555555555.log.q52fde6425d7406bdb19b936e1a1ba98c.chunk"),
        @klass.generate_queued_chunk_path(gen_path("mychunk.q55555555555555555555555555555555.log"), gen_test_chunk_id)
      )
    end

    test '.unique_id_from_path recreates unique_id from file path to assume unique_id for v0.12 chunks' do
      assert_equal gen_test_chunk_id, @klass.unique_id_from_path(gen_path("mychunk.q52fde6425d7406bdb19b936e1a1ba98c.log"))
    end
  end

  sub_test_case 'newly created chunk' do
    setup do
      @chunk_path = File.join(@chunkdir, 'test.*.log')
      @c = Fluent::Plugin::Buffer::FileChunk.new(gen_metadata, @chunk_path, :create)
    end

    def gen_chunk_path(prefix, unique_id)
      File.join(@chunkdir, "test.#{prefix}#{Fluent::UniqueId.hex(unique_id)}.log")
    end

    teardown do
      if @c
        @c.purge rescue nil
      end
      if File.exist? @chunk_path
        File.unlink @chunk_path
      end
    end

    test 'creates new files for chunk and metadata with specified path & permission' do
      assert{ @c.unique_id.size == 16 }
      assert_equal gen_chunk_path('b', @c.unique_id), @c.path

      assert File.exist?(gen_chunk_path('b', @c.unique_id))
      assert{ File.stat(gen_chunk_path('b', @c.unique_id)).mode.to_s(8).end_with?(@klass.const_get('FILE_PERMISSION').to_s(8)) }

      assert File.exist?(gen_chunk_path('b', @c.unique_id) + '.meta')
      assert{ File.stat(gen_chunk_path('b', @c.unique_id) + '.meta').mode.to_s(8).end_with?(@klass.const_get('FILE_PERMISSION').to_s(8)) }

      assert_equal :unstaged, @c.state
      assert @c.empty?
    end

    test 'can #append, #commit and #read it' do
      assert @c.empty?

      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"]
      @c.append(data)
      @c.commit

      content = @c.read
      ds = content.split("\n").select{|d| !d.empty? }

      assert_equal 2, ds.size
      assert_equal d1, JSON.parse(ds[0])
      assert_equal d2, JSON.parse(ds[1])

      d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
      d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @c.append([d3.to_json + "\n", d4.to_json + "\n"])
      @c.commit

      content = @c.read
      ds = content.split("\n").select{|d| !d.empty? }

      assert_equal 4, ds.size
      assert_equal d1, JSON.parse(ds[0])
      assert_equal d2, JSON.parse(ds[1])
      assert_equal d3, JSON.parse(ds[2])
      assert_equal d4, JSON.parse(ds[3])
    end

    test 'can #concat, #commit and #read it' do
      assert @c.empty?

      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"].join
      @c.concat(data, 2)
      @c.commit

      content = @c.read
      ds = content.split("\n").select{|d| !d.empty? }

      assert_equal 2, ds.size
      assert_equal d1, JSON.parse(ds[0])
      assert_equal d2, JSON.parse(ds[1])

      d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
      d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @c.concat([d3.to_json + "\n", d4.to_json + "\n"].join, 2)
      @c.commit

      content = @c.read
      ds = content.split("\n").select{|d| !d.empty? }

      assert_equal 4, ds.size
      assert_equal d1, JSON.parse(ds[0])
      assert_equal d2, JSON.parse(ds[1])
      assert_equal d3, JSON.parse(ds[2])
      assert_equal d4, JSON.parse(ds[3])
    end

    test 'has its contents in binary (ascii-8bit)' do
      data1 = "aaa bbb ccc".force_encoding('utf-8')
      @c.append([data1])
      @c.commit
      assert_equal Encoding::ASCII_8BIT, @c.instance_eval{ @chunk.external_encoding }

      content = @c.read
      assert_equal Encoding::ASCII_8BIT, content.encoding
    end

    test 'has #bytesize and #size' do
      assert @c.empty?

      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"]
      @c.append(data)

      assert_equal (d1.to_json + "\n" + d2.to_json + "\n").bytesize, @c.bytesize
      assert_equal 2, @c.size

      @c.commit

      assert_equal (d1.to_json + "\n" + d2.to_json + "\n").bytesize, @c.bytesize
      assert_equal 2, @c.size

      first_bytesize = @c.bytesize

      d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
      d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @c.append([d3.to_json + "\n", d4.to_json + "\n"])

      assert_equal first_bytesize + (d3.to_json + "\n" + d4.to_json + "\n").bytesize, @c.bytesize
      assert_equal 4, @c.size

      @c.commit

      assert_equal first_bytesize + (d3.to_json + "\n" + d4.to_json + "\n").bytesize, @c.bytesize
      assert_equal 4, @c.size
    end

    test 'can #rollback to revert non-committed data' do
      assert @c.empty?

      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"]
      @c.append(data)

      assert_equal (d1.to_json + "\n" + d2.to_json + "\n").bytesize, @c.bytesize
      assert_equal 2, @c.size

      @c.rollback

      assert @c.empty?

      assert_equal '', File.open(@c.path, 'rb'){|f| f.read }

      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"]
      @c.append(data)
      @c.commit

      assert_equal (d1.to_json + "\n" + d2.to_json + "\n").bytesize, @c.bytesize
      assert_equal 2, @c.size

      first_bytesize = @c.bytesize

      d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
      d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @c.append([d3.to_json + "\n", d4.to_json + "\n"])

      assert_equal first_bytesize + (d3.to_json + "\n" + d4.to_json + "\n").bytesize, @c.bytesize
      assert_equal 4, @c.size

      @c.rollback

      assert_equal first_bytesize, @c.bytesize
      assert_equal 2, @c.size

      assert_equal (d1.to_json + "\n" + d2.to_json + "\n"), File.open(@c.path, 'rb'){|f| f.read }
    end

    test 'can #rollback to revert non-committed data from #concat' do
      assert @c.empty?

      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"].join
      @c.concat(data, 2)

      assert_equal (d1.to_json + "\n" + d2.to_json + "\n").bytesize, @c.bytesize
      assert_equal 2, @c.size

      @c.rollback

      assert @c.empty?

      assert_equal '', File.open(@c.path, 'rb'){|f| f.read }

      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"]
      @c.append(data)
      @c.commit

      assert_equal (d1.to_json + "\n" + d2.to_json + "\n").bytesize, @c.bytesize
      assert_equal 2, @c.size

      first_bytesize = @c.bytesize

      d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
      d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @c.concat([d3.to_json + "\n", d4.to_json + "\n"].join, 2)

      assert_equal first_bytesize + (d3.to_json + "\n" + d4.to_json + "\n").bytesize, @c.bytesize
      assert_equal 4, @c.size

      @c.rollback

      assert_equal first_bytesize, @c.bytesize
      assert_equal 2, @c.size

      assert_equal (d1.to_json + "\n" + d2.to_json + "\n"), File.open(@c.path, 'rb'){|f| f.read }
    end

    test 'can store its data by #close' do
      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"]
      @c.append(data)
      @c.commit
      d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
      d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @c.append([d3.to_json + "\n", d4.to_json + "\n"])
      @c.commit

      content = @c.read

      unique_id = @c.unique_id
      size = @c.size
      created_at = @c.created_at
      modified_at = @c.modified_at

      @c.close

      assert_equal content, File.open(@c.path, 'rb'){|f| f.read }

      stored_meta = {
        timekey: nil, tag: nil, variables: nil,
        id: unique_id,
        s: size,
        c: created_at.to_i,
        m: modified_at.to_i,
      }

      assert_equal stored_meta, read_metadata_file(@c.path + '.meta')
    end

    test 'deletes all data by #purge' do
      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"]
      @c.append(data)
      @c.commit
      d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
      d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @c.append([d3.to_json + "\n", d4.to_json + "\n"])
      @c.commit

      @c.purge

      assert @c.empty?
      assert_equal 0, @c.bytesize
      assert_equal 0, @c.size

      assert !File.exist?(@c.path)
      assert !File.exist?(@c.path + '.meta')
    end

    test 'can #open its contents as io' do
      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"]
      @c.append(data)
      @c.commit
      d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
      d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @c.append([d3.to_json + "\n", d4.to_json + "\n"])
      @c.commit

      lines = []
      @c.open do |io|
        assert io
        io.readlines.each do |l|
          lines << l
        end
      end

      assert_equal d1.to_json + "\n", lines[0]
      assert_equal d2.to_json + "\n", lines[1]
      assert_equal d3.to_json + "\n", lines[2]
      assert_equal d4.to_json + "\n", lines[3]
    end

    test 'can refer system config for file permission' do
      omit "NTFS doesn't support UNIX like permissions" if Fluent.windows?

      chunk_path = File.join(@chunkdir, 'testperm.*.log')
      Fluent::SystemConfig.overwrite_system_config("file_permission" => "600") do
        c = Fluent::Plugin::Buffer::FileChunk.new(gen_metadata, chunk_path, :create)
        assert{ File.stat(c.path).mode.to_s(8).end_with?('600') }
        assert{ File.stat(c.path + '.meta').mode.to_s(8).end_with?('600') }
      end
    end

    test '#write_metadata tries to store metadata on file' do
      d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      data = [d1.to_json + "\n", d2.to_json + "\n"]
      @c.append(data)
      @c.commit

      expected = {
        timekey: nil, tag: nil, variables: nil,
        id: @c.unique_id,
        s: @c.size,
        c: @c.created_at.to_i,
        m: @c.modified_at.to_i,
      }
      assert_equal expected, read_metadata_file(@c.path + '.meta')

      d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
      d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @c.append([d3.to_json + "\n", d4.to_json + "\n"])
      # append does write_metadata

      dummy_now = Time.parse('2016-04-07 16:59:59 +0900')
      Timecop.freeze(dummy_now)
      @c.write_metadata

      expected = {
        timekey: nil, tag: nil, variables: nil,
        id: @c.unique_id,
        s: @c.size,
        c: @c.created_at.to_i,
        m: dummy_now.to_i,
      }
      assert_equal expected, read_metadata_file(@c.path + '.meta')

      @c.commit

      expected = {
        timekey: nil, tag: nil, variables: nil,
        id: @c.unique_id,
        s: @c.size,
        c: @c.created_at.to_i,
        m: @c.modified_at.to_i,
      }
      assert_equal expected, read_metadata_file(@c.path + '.meta')

      content = @c.read

      unique_id = @c.unique_id
      size = @c.size
      created_at = @c.created_at
      modified_at = @c.modified_at

      @c.close

      assert_equal content, File.open(@c.path, 'rb'){|f| f.read }

      stored_meta = {
        timekey: nil, tag: nil, variables: nil,
        id: unique_id,
        s: size,
        c: created_at.to_i,
        m: modified_at.to_i,
      }

      assert_equal stored_meta, read_metadata_file(@c.path + '.meta')
    end
  end

  sub_test_case 'chunk with file for staged chunk' do
    setup do
      @chunk_id = gen_test_chunk_id
      @chunk_path = File.join(@chunkdir, "test_staged.b#{hex_id(@chunk_id)}.log")
      @enqueued_path = File.join(@chunkdir, "test_staged.q#{hex_id(@chunk_id)}.log")

      @d1 = {"k" => "x", "f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      @d2 = {"k" => "x", "f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      @d3 = {"k" => "x", "f1" => 'x', "f2" => 'y', "f3" => 'z'}
      @d4 = {"k" => "x", "f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @d = [@d1,@d2,@d3,@d4].map{|d| d.to_json + "\n" }.join
      File.open(@chunk_path, 'wb') do |f|
        f.write @d
      end

      @metadata = {
        timekey: nil, tag: 'testing', variables: {k: "x"},
        id: @chunk_id,
        s: 4,
        c: Time.parse('2016-04-07 17:44:00 +0900').to_i,
        m: Time.parse('2016-04-07 17:44:13 +0900').to_i,
      }
      File.open(@chunk_path + '.meta', 'wb') do |f|
        f.write @metadata.to_msgpack
      end

      @c = Fluent::Plugin::Buffer::FileChunk.new(gen_metadata, @chunk_path, :staged)
    end

    teardown do
      if @c
        @c.purge rescue nil
      end
      [@chunk_path, @chunk_path + '.meta', @enqueued_path, @enqueued_path + '.meta'].each do |path|
        File.unlink path if File.exist? path
      end
    end

    test 'can load as staged chunk from file with metadata' do
      assert_equal @chunk_path, @c.path
      assert_equal :staged, @c.state

      assert_nil @c.metadata.timekey
      assert_equal 'testing', @c.metadata.tag
      assert_equal({k: "x"}, @c.metadata.variables)

      assert_equal 4, @c.size
      assert_equal Time.parse('2016-04-07 17:44:00 +0900'), @c.created_at
      assert_equal Time.parse('2016-04-07 17:44:13 +0900'), @c.modified_at

      content = @c.read
      assert_equal @d, content
    end

    test 'can be enqueued' do
      stage_path = @c.path
      queue_path = @enqueued_path
      assert File.exist?(stage_path)
      assert File.exist?(stage_path + '.meta')
      assert !File.exist?(queue_path)
      assert !File.exist?(queue_path + '.meta')

      @c.enqueued!

      assert_equal queue_path, @c.path

      assert !File.exist?(stage_path)
      assert !File.exist?(stage_path + '.meta')
      assert File.exist?(queue_path)
      assert File.exist?(queue_path + '.meta')

      assert_nil @c.metadata.timekey
      assert_equal 'testing', @c.metadata.tag
      assert_equal({k: "x"}, @c.metadata.variables)

      assert_equal 4, @c.size
      assert_equal Time.parse('2016-04-07 17:44:00 +0900'), @c.created_at
      assert_equal Time.parse('2016-04-07 17:44:13 +0900'), @c.modified_at

      assert_equal @d, File.open(@c.path, 'rb'){|f| f.read }
      assert_equal @metadata, read_metadata_file(@c.path + '.meta')
    end

    test '#write_metadata tries to store metadata on file with non-committed data' do
      d5 = {"k" => "x", "f1" => 'a', "f2" => 'b', "f3" => 'c'}
      d5s = d5.to_json + "\n"
      @c.append([d5s])

      metadata = {
        timekey: nil, tag: 'testing', variables: {k: "x"},
        id: @chunk_id,
        s: 4,
        c: Time.parse('2016-04-07 17:44:00 +0900').to_i,
        m: Time.parse('2016-04-07 17:44:13 +0900').to_i,
      }
      assert_equal metadata, read_metadata_file(@c.path + '.meta')

      @c.write_metadata

      metadata = {
        timekey: nil, tag: 'testing', variables: {k: "x"},
        id: @chunk_id,
        s: 5,
        c: Time.parse('2016-04-07 17:44:00 +0900').to_i,
        m: Time.parse('2016-04-07 17:44:38 +0900').to_i,
      }

      dummy_now = Time.parse('2016-04-07 17:44:38 +0900')
      Timecop.freeze(dummy_now)
      @c.write_metadata

      assert_equal metadata, read_metadata_file(@c.path + '.meta')
    end

    test '#file_rename can rename chunk files even in windows, and call callback with file size' do
      data = "aaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbccccccccccccccccccccccccccccc"

      testing_file1 = gen_path('rename1.test')
      testing_file2 = gen_path('rename2.test')
      f = File.open(testing_file1, 'wb', @c.permission)
      f.set_encoding(Encoding::ASCII_8BIT)
      f.sync = true
      f.binmode
      f.write data
      pos = f.pos

      assert f.binmode?
      assert f.sync
      assert_equal data.bytesize, f.size

      io = nil
      @c.file_rename(f, testing_file1, testing_file2, ->(new_io){ io = new_io })
      assert io
      if Fluent.windows?
        assert{ f != io }
      else
        assert_equal f, io
      end
      assert_equal Encoding::ASCII_8BIT, io.external_encoding
      assert io.sync
      assert io.binmode?
      assert_equal data.bytesize, io.size

      assert_equal pos, io.pos

      assert_equal '', io.read

      io.rewind
      assert_equal data, io.read
    end
  end

  sub_test_case 'chunk with file for enqueued chunk' do
    setup do
      @chunk_id = gen_test_chunk_id
      @enqueued_path = File.join(@chunkdir, "test_staged.q#{hex_id(@chunk_id)}.log")

      @d1 = {"k" => "x", "f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      @d2 = {"k" => "x", "f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      @d3 = {"k" => "x", "f1" => 'x', "f2" => 'y', "f3" => 'z'}
      @d4 = {"k" => "x", "f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @d = [@d1,@d2,@d3,@d4].map{|d| d.to_json + "\n" }.join
      File.open(@enqueued_path, 'wb') do |f|
        f.write @d
      end

      @dummy_timekey = Time.parse('2016-04-07 17:40:00 +0900').to_i

      @metadata = {
        timekey: @dummy_timekey, tag: 'testing', variables: {k: "x"},
        id: @chunk_id,
        s: 4,
        c: Time.parse('2016-04-07 17:44:00 +0900').to_i,
        m: Time.parse('2016-04-07 17:44:13 +0900').to_i,
      }
      File.open(@enqueued_path + '.meta', 'wb') do |f|
        f.write @metadata.to_msgpack
      end

      @c = Fluent::Plugin::Buffer::FileChunk.new(gen_metadata, @enqueued_path, :queued)
    end

    teardown do
      if @c
        @c.purge rescue nil
      end
      [@enqueued_path, @enqueued_path + '.meta'].each do |path|
        File.unlink path if File.exist? path
      end
    end

    test 'can load as queued chunk (read only) with metadata' do
      assert @c
      assert_equal @chunk_id, @c.unique_id
      assert_equal :queued, @c.state
      assert_equal gen_metadata(timekey: @dummy_timekey, tag: 'testing', variables: {k: "x"}), @c.metadata
      assert_equal Time.at(@metadata[:c]), @c.created_at
      assert_equal Time.at(@metadata[:m]), @c.modified_at
      assert_equal @metadata[:s], @c.size
      assert_equal @d.bytesize, @c.bytesize
      assert_equal @d, @c.read

      assert_raise RuntimeError.new("BUG: concatenating to unwritable chunk, now 'queued'") do
        @c.append(["queued chunk is read only"])
      end
      assert_raise IOError do
        @c.instance_eval{ @chunk }.write "chunk io is opened as read only"
      end
    end
  end

  sub_test_case 'chunk with queued chunk file of v0.12, without metadata' do
    setup do
      @chunk_id = gen_test_chunk_id
      @chunk_path = File.join(@chunkdir, "test_v12.2016040811.q#{hex_id(@chunk_id)}.log")

      @d1 = {"k" => "x", "f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      @d2 = {"k" => "x", "f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      @d3 = {"k" => "x", "f1" => 'x', "f2" => 'y', "f3" => 'z'}
      @d4 = {"k" => "x", "f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @d = [@d1,@d2,@d3,@d4].map{|d| d.to_json + "\n" }.join
      File.open(@chunk_path, 'wb') do |f|
        f.write @d
      end

      @c = Fluent::Plugin::Buffer::FileChunk.new(gen_metadata, @chunk_path, :queued)
    end

    teardown do
      if @c
        @c.purge rescue nil
      end
      File.unlink @chunk_path if File.exist? @chunk_path
    end

    test 'can load as queued chunk from file without metadata' do
      assert @c
      assert_equal :queued, @c.state
      assert_equal @chunk_id, @c.unique_id
      assert_equal gen_metadata, @c.metadata
      assert_equal @d.bytesize, @c.bytesize
      assert_equal 0, @c.size
      assert_equal @d, @c.read

      assert_raise RuntimeError.new("BUG: concatenating to unwritable chunk, now 'queued'") do
        @c.append(["queued chunk is read only"])
      end
      assert_raise IOError do
        @c.instance_eval{ @chunk }.write "chunk io is opened as read only"
      end
    end
  end

  sub_test_case 'chunk with staged chunk file of v0.12, without metadata' do
    setup do
      @chunk_id = gen_test_chunk_id
      @chunk_path = File.join(@chunkdir, "test_v12.2016040811.b#{hex_id(@chunk_id)}.log")

      @d1 = {"k" => "x", "f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
      @d2 = {"k" => "x", "f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
      @d3 = {"k" => "x", "f1" => 'x', "f2" => 'y', "f3" => 'z'}
      @d4 = {"k" => "x", "f1" => 'a', "f2" => 'b', "f3" => 'c'}
      @d = [@d1,@d2,@d3,@d4].map{|d| d.to_json + "\n" }.join
      File.open(@chunk_path, 'wb') do |f|
        f.write @d
      end

      @c = Fluent::Plugin::Buffer::FileChunk.new(gen_metadata, @chunk_path, :staged)
    end

    teardown do
      if @c
        @c.purge rescue nil
      end
      File.unlink @chunk_path if File.exist? @chunk_path
    end

    test 'can load as queued chunk from file without metadata even if it was loaded as staged chunk' do
      assert @c
      assert_equal :queued, @c.state
      assert_equal @chunk_id, @c.unique_id
      assert_equal gen_metadata, @c.metadata
      assert_equal @d.bytesize, @c.bytesize
      assert_equal 0, @c.size
      assert_equal @d, @c.read

      assert_raise RuntimeError.new("BUG: concatenating to unwritable chunk, now 'queued'") do
        @c.append(["queued chunk is read only"])
      end
      assert_raise IOError do
        @c.instance_eval{ @chunk }.write "chunk io is opened as read only"
      end
    end
  end

  sub_test_case 'compressed buffer' do
    setup do
      @src = 'text data for compressing' * 5
      @gzipped_src = compress(@src)
    end

    test '#append with compress option writes  compressed data to chunk when compress is gzip' do
      c = @klass.new(gen_metadata, File.join(@chunkdir,'test.*.log'), :create, compress: :gzip)
      c.append([@src, @src], compress: :gzip)
      c.commit

      # check chunk is compressed
      assert c.read(compressed: :gzip).size < [@src, @src].join("").size

      assert_equal @src + @src, c.read
    end

    test '#open passes io object having decompressed data to a block when compress is gzip' do
      c = @klass.new(gen_metadata, File.join(@chunkdir,'test.*.log'), :create, compress: :gzip)
      c.concat(@gzipped_src, @src.size)
      c.commit

      decomressed_data = c.open do |io|
        v = io.read
        assert_equal @src, v
        v
      end
      assert_equal @src, decomressed_data
    end

    test '#open with compressed option passes io object having decompressed data to a block when compress is gzip' do
      c = @klass.new(gen_metadata, File.join(@chunkdir,'test.*.log'), :create, compress: :gzip)
      c.concat(@gzipped_src, @src.size)
      c.commit

      comressed_data = c.open(compressed: :gzip) do |io|
        v = io.read
        assert_equal @gzipped_src, v
        v
      end
      assert_equal @gzipped_src, comressed_data
    end

    test '#write_to writes decompressed data when compress is gzip' do
      c = @klass.new(gen_metadata, File.join(@chunkdir,'test.*.log'), :create, compress: :gzip)
      c.concat(@gzipped_src, @src.size)
      c.commit

      assert_equal @src, c.read
      assert_equal @gzipped_src, c.read(compressed: :gzip)

      io = StringIO.new
      c.write_to(io)
      assert_equal @src, io.string
    end

    test '#write_to with compressed option writes compressed data when compress is gzip' do
      c = @klass.new(gen_metadata, File.join(@chunkdir,'test.*.log'), :create, compress: :gzip)
      c.concat(@gzipped_src, @src.size)
      c.commit

      assert_equal @src, c.read
      assert_equal @gzipped_src, c.read(compressed: :gzip)

      io = StringIO.new
      c.write_to(io, compressed: :gzip)
      assert_equal @gzipped_src, io.string
    end
  end
end
