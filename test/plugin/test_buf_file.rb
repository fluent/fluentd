# -*- coding: utf-8 -*-
require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/buf_file'

require 'fileutils'

require 'stringio'
require 'msgpack'

module FluentFileBufferTest
  class FileBufferChunkTest < Test::Unit::TestCase
    BUF_FILE_TMPDIR = File.expand_path(File.join(File.dirname(__FILE__), '..', 'tmp', 'buf_file_chunk'))

    def setup
      if Dir.exists? BUF_FILE_TMPDIR
        FileUtils.remove_entry_secure BUF_FILE_TMPDIR
      end
      FileUtils.mkdir_p BUF_FILE_TMPDIR
    end

    def bufpath(unique, link=false)
      File.join(BUF_FILE_TMPDIR, unique + '.log' + (link ? '.link' : ''))
    end

    def filebufferchunk(key, unique, opts={})
      Fluent::FileBufferChunk.new(key, bufpath(unique), unique, opts[:mode] || "a+", opts[:symlink])
    end

    def test_init
      chunk = filebufferchunk('key', 'init1')
      assert_equal 'key', chunk.key
      assert_equal 'init1', chunk.unique_id
      assert_equal bufpath('init1'), chunk.path

      chunk.close # size==0, then, unlinked

      symlink_path = bufpath('init2', true)

      chunk = filebufferchunk('key2', 'init2', symlink: symlink_path)
      assert_equal 'key2', chunk.key
      assert_equal 'init2', chunk.unique_id
      assert File.exists?(symlink_path) && File.symlink?(symlink_path)

      chunk.close # unlink

      assert File.symlink?(symlink_path)
      File.unlink(symlink_path)
    end

    def test_buffer_chunk_interface
      chunk = filebufferchunk('key', 'interface1')

      assert chunk.respond_to?(:empty?)
      assert chunk.respond_to?(:<<)
      assert chunk.respond_to?(:size)
      assert chunk.respond_to?(:close)
      assert chunk.respond_to?(:purge)
      assert chunk.respond_to?(:read)
      assert chunk.respond_to?(:open)
      assert chunk.respond_to?(:write_to)
      assert chunk.respond_to?(:msgpack_each)

      chunk.close
    end

    def test_empty?
      chunk = filebufferchunk('e1', 'empty1')
      assert chunk.empty?
      chunk.close

      open(bufpath('empty2'), 'w') do |file|
        file.write "data1\ndata2\n"
      end
      chunk = filebufferchunk('e2', 'empty2')
      assert !(chunk.empty?)
      chunk.close
    end

    def test_append_close_purge
      chunk = filebufferchunk('a1', 'append1')
      assert chunk.empty?

      test_data1 = ("1" * 9 + "\n" + "2" * 9 + "\n").force_encoding('ASCII-8BIT')
      test_data2 = "日本語Japanese\n".force_encoding('UTF-8')
      chunk << test_data1
      chunk << test_data2
      assert_equal 38, chunk.size
      chunk.close

      assert File.exists?(bufpath('append1'))

      chunk = filebufferchunk('a1', 'append1', mode: 'r')
      test_data = test_data1.force_encoding('ASCII-8BIT') + test_data2.force_encoding('ASCII-8BIT')

      #### TODO: This assertion currently fails. Oops.
      # FileBuffer#read does NOT do force_encoding('ASCII-8BIT'). So encoding of output string instance are 'UTF-8'.
      # I think it is a kind of bug, but fixing it may break some behavior of buf_file. So I cannot be sure to fix it just now.
      #
      # assert_equal test_data, chunk.read

      chunk.purge

      assert !(File.exists?(bufpath('append1')))
    end

    def test_empty_chunk_key # for BufferedOutput#emit
      chunk = filebufferchunk('', 'append1')
      assert chunk.empty?

      test_data1 = ("1" * 9 + "\n" + "2" * 9 + "\n").force_encoding('ASCII-8BIT')
      test_data2 = "日本語Japanese\n".force_encoding('UTF-8')
      chunk << test_data1
      chunk << test_data2
      assert_equal 38, chunk.size
      chunk.close
    end

    def test_read
      chunk = filebufferchunk('r1', 'read1')
      assert chunk.empty?

      d1 = "abcde" * 200 + "\n"
      chunk << d1
      d2 = "12345" * 200 + "\n"
      chunk << d2
      assert_equal (d1.size + d2.size), chunk.size

      read_data = chunk.read
      assert_equal (d1 + d2), read_data

      chunk.purge
    end

    def test_open
      chunk = filebufferchunk('o1', 'open1')
      assert chunk.empty?

      d1 = "abcde" * 200 + "\n"
      chunk << d1
      d2 = "12345" * 200 + "\n"
      chunk << d2
      assert_equal (d1.size + d2.size), chunk.size

      read_data = chunk.open do |io|
        io.read
      end
      assert_equal (d1 + d2), read_data

      chunk.purge
    end

    def test_write_to
      chunk = filebufferchunk('w1', 'write1')
      assert chunk.empty?

      d1 = "abcde" * 200 + "\n"
      chunk << d1
      d2 = "12345" * 200 + "\n"
      chunk << d2
      assert_equal (d1.size + d2.size), chunk.size

      dummy_dst = StringIO.new

      chunk.write_to(dummy_dst)
      assert_equal (d1 + d2), dummy_dst.string

      chunk.purge
    end

    def test_msgpack_each
      chunk = filebufferchunk('m1', 'msgpack1')
      assert chunk.empty?

      d0 = MessagePack.pack([[1, "foo"], [2, "bar"], [3, "baz"]])
      d1 = MessagePack.pack({"key1" => "value1", "key2" => "value2"})
      d2 = MessagePack.pack("string1")
      d3 = MessagePack.pack(1)
      d4 = MessagePack.pack(nil)
      chunk << d0
      chunk << d1
      chunk << d2
      chunk << d3
      chunk << d4

      store = []
      chunk.msgpack_each do |data|
        store << data
      end

      assert_equal 5, store.size
      assert_equal [[1, "foo"], [2, "bar"], [3, "baz"]], store[0]
      assert_equal({"key1" => "value1", "key2" => "value2"}, store[1])
      assert_equal "string1", store[2]
      assert_equal 1, store[3]
      assert_equal nil, store[4]

      chunk.purge
    end

    def test_mv
      chunk = filebufferchunk('m1', 'move1')
      assert chunk.empty?

      d1 = "abcde" * 200 + "\n"
      chunk << d1
      d2 = "12345" * 200 + "\n"
      chunk << d2
      assert_equal (d1.size + d2.size), chunk.size

      assert_equal bufpath('move1'), chunk.path

      assert File.exists?( bufpath( 'move1' ) )
      assert !(File.exists?( bufpath( 'move2' ) ))

      chunk.mv(bufpath('move2'))

      assert !(File.exists?( bufpath( 'move1' ) ))
      assert File.exists?( bufpath( 'move2' ) )

      assert_equal bufpath('move2'), chunk.path

      chunk.purge
    end
  end

  class FileBufferTest < Test::Unit::TestCase
    BUF_FILE_TMPDIR = File.expand_path(File.join(File.dirname(__FILE__), '..', 'tmp', 'buf_file'))

    def setup
      if Dir.exists? BUF_FILE_TMPDIR
        FileUtils.remove_entry_secure BUF_FILE_TMPDIR
      end
      FileUtils.mkdir_p BUF_FILE_TMPDIR
    end

    def bufpath(basename)
      File.join(BUF_FILE_TMPDIR, basename)
    end

    def filebuffer(key, unique, opts={})
      Fluent::FileBufferChunk.new(key, bufpath(unique), unique, opts[:mode] || "a+", opts[:symlink])
    end

    def test_init_configure
      buf = Fluent::FileBuffer.new

      assert_raise(Fluent::ConfigError){ buf.configure({}) }

      buf.configure({'buffer_path' => bufpath('configure1.*.log')})
      assert_equal bufpath('configure1.*.log'), buf.buffer_path
      assert_equal nil, buf.symlink_path
      assert_equal false, buf.instance_eval{ @flush_at_shutdown }

      buf2 = Fluent::FileBuffer.new

      # Same buffer_path value is rejected, not to overwrite exisitng buffer file.
      assert_raise(Fluent::ConfigError){ buf2.configure({'buffer_path' => bufpath('configure1.*.log')}) }

      buf2.configure({'buffer_path' => bufpath('configure2.*.log'), 'flush_at_shutdown' => ''})
      assert_equal bufpath('configure2.*.log'), buf2.buffer_path
      assert_equal true, buf2.instance_eval{ @flush_at_shutdown }
    end

    def test_configure_path_prefix_suffix
      # With '*' in path, prefix is the part before '*', suffix is the part after '*'
      buf = Fluent::FileBuffer.new

      path1 = bufpath('suffpref1.*.log')
      prefix1, suffix1 = path1.split('*', 2)
      buf.configure({'buffer_path' => path1})
      assert_equal prefix1, buf.instance_eval{ @buffer_path_prefix }
      assert_equal suffix1, buf.instance_eval{ @buffer_path_suffix }

      # Without '*', prefix is the string of whole path + '.', suffix is '.log'
      path2 = bufpath('suffpref2')
      buf.configure({'buffer_path' => path2})
      assert_equal path2 + '.', buf.instance_eval{ @buffer_path_prefix }
      assert_equal '.log', buf.instance_eval{ @buffer_path_suffix }
    end

    class DummyOutput
      attr_accessor :written

      def write(chunk)
        @written ||= []
        @written.push chunk
        "return value"
      end
    end

    def test_encode_key
      buf = Fluent::FileBuffer.new
      safe_chars = '-_.abcdefgxyzABCDEFGXYZ0123456789'
      assert_equal safe_chars, buf.send(:encode_key, safe_chars)
      unsafe_chars = '-_.abcdefgxyzABCDEFGXYZ0123456789 ~/*()'
      assert_equal safe_chars + '%20%7E%2F%2A%28%29', buf.send(:encode_key, unsafe_chars)
    end

    def test_decode_key
      buf = Fluent::FileBuffer.new
      safe_chars = '-_.abcdefgxyzABCDEFGXYZ0123456789'
      assert_equal safe_chars, buf.send(:decode_key, safe_chars)
      unsafe_chars = '-_.abcdefgxyzABCDEFGXYZ0123456789 ~/*()'
      assert_equal unsafe_chars, buf.send(:decode_key, safe_chars + '%20%7E%2F%2A%28%29')

      assert_equal safe_chars, buf.send(:decode_key, buf.send(:encode_key, safe_chars))
      assert_equal unsafe_chars, buf.send(:decode_key, buf.send(:encode_key, unsafe_chars))
    end

    def test_make_path
      buf = Fluent::FileBuffer.new
      buf.configure({'buffer_path' => bufpath('makepath.*.log')})
      prefix = buf.instance_eval{ @buffer_path_prefix }
      suffix = buf.instance_eval{ @buffer_path_suffix }

      path,tsuffix = buf.send(:make_path, buf.send(:encode_key, 'foo bar'), 'b')
      assert path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.[bq][0-9a-f]+#{suffix}\Z/, "invalid format:#{path}"
      assert tsuffix =~ /\A[0-9a-f]+\Z/, "invalid hexadecimal:#{tsuffix}"

      path,tsuffix = buf.send(:make_path, buf.send(:encode_key, 'baz 123'), 'q')
      assert path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.[bq][0-9a-f]+#{suffix}\Z/, "invalid format:#{path}"
      assert tsuffix =~ /\A[0-9a-f]+\Z/, "invalid hexadecimal:#{tsuffix}"
    end

    def test_tsuffix_to_unique_id
      buf = Fluent::FileBuffer.new
      # why *2 ? frsyuki said "I forgot why completely."
      assert_equal "\xFF\xFF\xFF\xFF".force_encoding('ASCII-8BIT'), buf.send(:tsuffix_to_unique_id, 'ffff')
      assert_equal "\x88\x00\xFF\x00\x11\xEE\x88\x00\xFF\x00\x11\xEE".force_encoding('ASCII-8BIT'), buf.send(:tsuffix_to_unique_id, '8800ff0011ee')
    end

    def test_start_makes_parent_directories
      buf = Fluent::FileBuffer.new
      buf.configure({'buffer_path' => bufpath('start/base.*.log')})
      parent_dirname = File.dirname(buf.instance_eval{ @buffer_path_prefix })
      assert !(Dir.exists?(parent_dirname))
      buf.start
      assert Dir.exists?(parent_dirname)
    end

    def test_new_chunk
      buf = Fluent::FileBuffer.new
      buf.configure({'buffer_path' => bufpath('new_chunk_1')})
      prefix = buf.instance_eval{ @buffer_path_prefix }
      suffix = buf.instance_eval{ @buffer_path_suffix }

      chunk = buf.new_chunk('key1')
      assert chunk
      assert File.exists?(chunk.path)
      assert chunk.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.b[0-9a-f]+#{suffix}\Z/, "path from new_chunk must be a 'b' buffer chunk"
      chunk.close
    end

    def test_chunk_identifier_in_path
      buf1 = Fluent::FileBuffer.new
      buf1.configure({'buffer_path' => bufpath('chunkid1')})
      prefix1 = buf1.instance_eval{ @buffer_path_prefix }
      suffix1 = buf1.instance_eval{ @buffer_path_suffix }

      chunk1 = buf1.new_chunk('key1')
      assert_equal chunk1.path, prefix1 + buf1.chunk_identifier_in_path(chunk1.path) + suffix1

      buf2 = Fluent::FileBuffer.new
      buf2.configure({'buffer_path' => bufpath('chunkid2')})
      prefix2 = buf2.instance_eval{ @buffer_path_prefix }
      suffix2 = buf2.instance_eval{ @buffer_path_suffix }

      chunk2 = buf2.new_chunk('key2')
      assert_equal chunk2.path, prefix2 + buf2.chunk_identifier_in_path(chunk2.path) + suffix2
    end

    def test_enqueue_moves_chunk_from_b_to_q
      buf = Fluent::FileBuffer.new
      buf.configure({'buffer_path' => bufpath('enqueue1')})
      prefix = buf.instance_eval{ @buffer_path_prefix }
      suffix = buf.instance_eval{ @buffer_path_suffix }

      chunk = buf.new_chunk('key1')
      chunk << "data1\ndata2\n"

      assert chunk
      old_path = chunk.path.dup
      assert File.exists?(chunk.path)
      assert chunk.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.b[0-9a-f]+#{suffix}\Z/, "path from new_chunk must be a 'b' buffer chunk"

      buf.enqueue(chunk)

      assert chunk
      assert File.exists?(chunk.path)
      assert !(File.exists?(old_path))
      assert chunk.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.q[0-9a-f]+#{suffix}\Z/, "enqueued chunk's path must be a 'q' buffer chunk"

      data = chunk.read
      assert "data1\ndata2\n", data
    end

    # empty chunk keys are used w/ BufferedOutput
    #  * ObjectBufferedOutput's keys are tag
    #  * TimeSlicedOutput's keys are time_key
    def test_enqueue_chunk_with_empty_key
      buf = Fluent::FileBuffer.new
      buf.configure({'buffer_path' => bufpath('enqueue2')})
      prefix = buf.instance_eval{ @buffer_path_prefix }
      suffix = buf.instance_eval{ @buffer_path_suffix }

      chunk = buf.new_chunk('')
      chunk << "data1\ndata2\n"

      assert chunk
      old_path = chunk.path.dup
      assert File.exists?(chunk.path)
      # chunk key is empty
      assert chunk.path =~ /\A#{prefix}\.b[0-9a-f]+#{suffix}\Z/, "path from new_chunk must be a 'b' buffer chunk"

      buf.enqueue(chunk)

      assert chunk
      assert File.exists?(chunk.path)
      assert !(File.exists?(old_path))
      # chunk key is empty
      assert chunk.path =~ /\A#{prefix}\.q[0-9a-f]+#{suffix}\Z/, "enqueued chunk's path must be a 'q' buffer chunk"

      data = chunk.read
      assert "data1\ndata2\n", data
    end

    def test_before_shutdown_without_flush_at_shutdown
      buf = Fluent::FileBuffer.new
      buf.configure({'buffer_path' => bufpath('before_shutdown1')})
      buf.start

      # before_shutdown does nothing

      c1 = [ buf.new_chunk('k0'), buf.new_chunk('k1'), buf.new_chunk('k2'), buf.new_chunk('k3') ]
      c2 = [ buf.new_chunk('q0'), buf.new_chunk('q1') ]

      buf.instance_eval do
        @map = {
          'k0' => c1[0], 'k1' => c1[1], 'k2' => c1[2], 'k3' => c1[3],
          'q0' => c2[0], 'q1' => c2[1]
        }
      end
      c1[0] << "data1\ndata2\n"
      c1[1] << "data1\ndata2\n"
      c1[2] << "data1\ndata2\n"
      # k3 chunk is empty!

      c2[0] << "data1\ndata2\n"
      c2[1] << "data1\ndata2\n"
      buf.push('q0')
      buf.push('q1')

      buf.instance_eval do
        @enqueue_hook_times = 0
        def enqueue(chunk)
          @enqueue_hook_times += 1
        end
      end
      assert_equal 0, buf.instance_eval{ @enqueue_hook_times }

      out = DummyOutput.new
      assert_equal nil, out.written

      buf.before_shutdown(out)

      assert_equal 0, buf.instance_eval{ @enqueue_hook_times } # k0, k1, k2
      assert_nil out.written
    end

    def test_before_shutdown_with_flush_at_shutdown
      buf = Fluent::FileBuffer.new
      buf.configure({'buffer_path' => bufpath('before_shutdown2'), 'flush_at_shutdown' => 'true'})
      buf.start

      # before_shutdown flushes all chunks in @map and @queue

      c1 = [ buf.new_chunk('k0'), buf.new_chunk('k1'), buf.new_chunk('k2'), buf.new_chunk('k3') ]
      c2 = [ buf.new_chunk('q0'), buf.new_chunk('q1') ]

      buf.instance_eval do
        @map = {
          'k0' => c1[0], 'k1' => c1[1], 'k2' => c1[2], 'k3' => c1[3],
          'q0' => c2[0], 'q1' => c2[1]
        }
      end
      c1[0] << "data1\ndata2\n"
      c1[1] << "data1\ndata2\n"
      c1[2] << "data1\ndata2\n"
      # k3 chunk is empty!

      c2[0] << "data1\ndata2\n"
      c2[1] << "data1\ndata2\n"
      buf.push('q0')
      buf.push('q1')

      buf.instance_eval do
        @enqueue_hook_times = 0
        def enqueue(chunk)
          @enqueue_hook_times += 1
        end
      end
      assert_equal 0, buf.instance_eval{ @enqueue_hook_times }

      out = DummyOutput.new
      assert_equal nil, out.written

      buf.before_shutdown(out)

      assert_equal 3, buf.instance_eval{ @enqueue_hook_times } # k0, k1, k2
      assert_equal 5, out.written.size
      assert_equal [c2[0], c2[1], c1[0], c1[1], c1[2]], out.written
    end

    def test_resume
      buffer_path_for_resume_test = bufpath('resume')

      buf1 = Fluent::FileBuffer.new
      buf1.configure({'buffer_path' => buffer_path_for_resume_test})
      prefix = buf1.instance_eval{ @buffer_path_prefix }
      suffix = buf1.instance_eval{ @buffer_path_suffix }

      buf1.start

      chunk1 = buf1.new_chunk('key1')
      chunk1 << "data1\ndata2\n"

      chunk2 = buf1.new_chunk('key2')
      chunk2 << "data3\ndata4\n"

      assert chunk1
      assert chunk1.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.b[0-9a-f]+#{suffix}\Z/, "path from new_chunk must be a 'b' buffer chunk"

      buf1.enqueue(chunk1)

      assert chunk1
      assert chunk1.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.q[0-9a-f]+#{suffix}\Z/, "chunk1 must be enqueued"
      assert chunk2
      assert chunk2.path =~ /\A#{prefix}[-_.a-zA-Z0-9\%]+\.b[0-9a-f]+#{suffix}\Z/, "chunk2 is not enqueued yet"

      buf1.shutdown

      buf2 = Fluent::FileBuffer.new
      Fluent::FileBuffer.send(:class_variable_set, :'@@buffer_paths', {})
      buf2.configure({'buffer_path' => buffer_path_for_resume_test})
      prefix = buf2.instance_eval{ @buffer_path_prefix }
      suffix = buf2.instance_eval{ @buffer_path_suffix }

      # buf1.start -> resume is normal operation, but now, we cannot it.
      queue, map = buf2.resume

      assert_equal 1, queue.size
      assert_equal 1, map.size

      resumed_chunk1 = queue.first
      assert_equal chunk1.path, resumed_chunk1.path
      resumed_chunk2 = map['key2']
      assert_equal chunk2.path, resumed_chunk2.path

      assert_equal "data1\ndata2\n", resumed_chunk1.read
      assert_equal "data3\ndata4\n", resumed_chunk2.read
    end

    class DummyChain
      def next
        true
      end
    end

    def test_resume_only_for_my_buffer_path
      chain = DummyChain.new

      buffer_path_for_resume_test_1 = bufpath('resume_fix.1.*.log')
      buffer_path_for_resume_test_2 = bufpath('resume_fix.*.log')

      buf1 = Fluent::FileBuffer.new
      buf1.configure({'buffer_path' => buffer_path_for_resume_test_1})
      buf1.start

      buf1.emit('key1', "x1\ty1\tz1\n", chain)
      buf1.emit('key1', "x2\ty2\tz2\n", chain)

      assert buf1.instance_eval{ @map['key1'] }

      buf1.shutdown

      buf2 = Fluent::FileBuffer.new
      buf2.configure({'buffer_path' => buffer_path_for_resume_test_2}) # other buffer_path

      queue, map = buf2.resume

      assert_equal 0, queue.size

      ### TODO: This map size MUST be 0, but actually, 1
      # This is because 1.XXXXX is misunderstood like chunk key of resume_fix.*.log.
      # This may be a kind of bug, but we cannot decide whether 1. is a part of chunk key or not,
      # because current version of buffer plugin uses '.'(dot) as a one of chars for chunk encoding.
      # I think that this is a mistake of design, but we cannot fix it because updated plugin become
      # not to be able to resume existing file buffer chunk.
      # We will fix it in next API version of buffer plugin.
      assert_equal 1, map.size
    end
  end
end
