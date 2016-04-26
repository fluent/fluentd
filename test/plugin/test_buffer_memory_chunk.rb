require_relative '../helper'
require 'fluent/plugin/buffer/memory_chunk'

require 'json'

class BufferMemoryChunkTest < Test::Unit::TestCase
  setup do
    @c = Fluent::Plugin::Buffer::MemoryChunk.new(Object.new)
  end

  test 'has blank chunk initially' do
    assert @c.empty?
    assert_equal '', @c.instance_eval{ @chunk }
    assert_equal 0, @c.instance_eval{ @chunk_bytes }
    assert_equal 0, @c.instance_eval{ @adding_bytes }
    assert_equal 0, @c.instance_eval{ @adding_records }
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
    assert_equal Encoding::ASCII_8BIT, @c.instance_eval{ @chunk.encoding }

    content = @c.read
    assert_equal Encoding::ASCII_8BIT, content.encoding
  end

  test 'has #size and #records' do
    assert @c.empty?

    d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
    d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
    data = [d1.to_json + "\n", d2.to_json + "\n"]
    @c.append(data)

    assert_equal (d1.to_json + "\n" + d2.to_json + "\n").size, @c.size
    assert_equal 2, @c.records

    @c.commit

    assert_equal (d1.to_json + "\n" + d2.to_json + "\n").size, @c.size
    assert_equal 2, @c.records

    first_size = @c.size

    d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
    d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
    @c.append([d3.to_json + "\n", d4.to_json + "\n"])

    assert_equal first_size + (d3.to_json + "\n" + d4.to_json + "\n").size, @c.size
    assert_equal 4, @c.records

    @c.commit

    assert_equal first_size + (d3.to_json + "\n" + d4.to_json + "\n").size, @c.size
    assert_equal 4, @c.records
  end

  test 'can #rollback to revert non-committed data' do
    assert @c.empty?

    d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
    d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
    data = [d1.to_json + "\n", d2.to_json + "\n"]
    @c.append(data)

    assert_equal (d1.to_json + "\n" + d2.to_json + "\n").size, @c.size
    assert_equal 2, @c.records

    @c.rollback

    assert @c.empty?

    assert @c.empty?

    d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
    d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
    data = [d1.to_json + "\n", d2.to_json + "\n"]
    @c.append(data)
    @c.commit

    assert_equal (d1.to_json + "\n" + d2.to_json + "\n").size, @c.size
    assert_equal 2, @c.records

    first_size = @c.size

    d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
    d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
    @c.append([d3.to_json + "\n", d4.to_json + "\n"])

    assert_equal first_size + (d3.to_json + "\n" + d4.to_json + "\n").size, @c.size
    assert_equal 4, @c.records

    @c.rollback

    assert_equal first_size, @c.size
    assert_equal 2, @c.records
  end

  test 'can #rollback to revert non-committed data from #concat' do
    assert @c.empty?

    d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
    d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
    data = [d1.to_json + "\n", d2.to_json + "\n"].join
    @c.concat(data, 2)

    assert_equal (d1.to_json + "\n" + d2.to_json + "\n").size, @c.size
    assert_equal 2, @c.records

    @c.rollback

    assert @c.empty?

    assert @c.empty?

    d1 = {"f1" => 'v1', "f2" => 'v2', "f3" => 'v3'}
    d2 = {"f1" => 'vv1', "f2" => 'vv2', "f3" => 'vv3'}
    data = [d1.to_json + "\n", d2.to_json + "\n"]
    @c.append(data)
    @c.commit

    assert_equal (d1.to_json + "\n" + d2.to_json + "\n").size, @c.size
    assert_equal 2, @c.records

    first_size = @c.size

    d3 = {"f1" => 'x', "f2" => 'y', "f3" => 'z'}
    d4 = {"f1" => 'a', "f2" => 'b', "f3" => 'c'}
    @c.concat([d3.to_json + "\n", d4.to_json + "\n"].join, 2)

    assert_equal first_size + (d3.to_json + "\n" + d4.to_json + "\n").size, @c.size
    assert_equal 4, @c.records

    @c.rollback

    assert_equal first_size, @c.size
    assert_equal 2, @c.records
  end

  test 'does nothing for #close' do
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

    @c.close

    assert_equal content, @c.read
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
    assert_equal 0, @c.size
    assert_equal 0, @c.records
    assert_equal '', @c.read
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
end
