require_relative '../../helper'
require 'fluent/test/driver/output'
require 'flexmock/test_unit'

require 'fluent/plugin/out_forward'
require 'fluent/plugin/out_forward/ack_handler'

class AckHandlerTest < Test::Unit::TestCase
  data(
    'chunk_id is matched' => [MessagePack.pack({ 'ack' => Base64.encode64('chunk_id 111') }), Fluent::Plugin::ForwardOutput::AckHandler::Result::SUCCESS],
    'chunk_id is not matched' => [MessagePack.pack({ 'ack' => 'unmatched' }), Fluent::Plugin::ForwardOutput::AckHandler::Result::CHUNKID_UNMATCHED],
    'chunk_id is empty' => ['', Fluent::Plugin::ForwardOutput::AckHandler::Result::FAILED],
    'connection is closed with nil EOF' => [nil, Fluent::Plugin::ForwardOutput::AckHandler::Result::FAILED],
  )
  test 'returns chunk_id, node, sock and result status' do |args|
    receved, state = args
    ack_handler = Fluent::Plugin::ForwardOutput::AckHandler.new(timeout: 10, log: $log, read_length: 100)

    node = flexmock('node', host: '127.0.0.1', port: '1000') # for log
    chunk_id = 'chunk_id 111'
    ack = ack_handler.create_ack(chunk_id, node)

    r, w = IO.pipe
    begin
      w.write(chunk_id)
      mock(r).recv(anything) { |_| receved } # IO does not have recv
      ack.enqueue(r)

      a1 = a2 = a3 = a4 = nil
      ack_handler.collect_response(1) do |cid, n, s, ret|
        # This block is rescued by ack_handler so it needs to invoke assertion outside of this block
        a1 = cid; a2 = n; a3 = s; a4 = ret
      end

      assert_equal chunk_id, a1
      assert_equal node, a2
      assert_equal r, a3
      assert_equal state, a4
    ensure
      r.close rescue nil
      w.close rescue nil
    end
  end

  test 'returns nil if raise an error' do
    ack_handler = Fluent::Plugin::ForwardOutput::AckHandler.new(timeout: 10, log: $log, read_length: 100)

    node = flexmock('node', host: '127.0.0.1', port: '1000') # for log
    chunk_id = 'chunk_id 111'
    ack = ack_handler.create_ack(chunk_id, node)

    r, w = IO.pipe
    begin
      w.write(chunk_id)
      mock(r).recv(anything) { |_| raise 'unexpected error' } # IO does not have recv
      ack.enqueue(r)

      a1 = a2 = a3 = a4 = nil
      ack_handler.collect_response(1) do |cid, n, s, ret|
        # This block is rescued by ack_handler so it needs to invoke assertion outside of this block
        a1 = cid; a2 = n; a3 = s; a4 = ret
      end

      assert_nil a1
      assert_nil a2
      assert_nil a3
      assert_equal Fluent::Plugin::ForwardOutput::AckHandler::Result::FAILED, a4
    ensure
      r.close rescue nil
      w.close rescue nil
    end
  end

  data(
    'truncated response' => [MessagePack.pack({ 'ack' => Base64.encode64('chunk_id 111') })[0..5], Fluent::Plugin::ForwardOutput::AckHandler::Result::FAILED],
    'extra bytes after the response' => [MessagePack.pack({ 'ack' => Base64.encode64('chunk_id 111') }) + MessagePack.pack({ 'ack' => 'extra' }), Fluent::Plugin::ForwardOutput::AckHandler::Result::SUCCESS],
  )
  test 'a response on a connection does not affect the ack of another connection' do |args|
    first_response, first_result = args
    # A truncated response logs an error. Keep it out of $log, which another test checks.
    ack_handler = Fluent::Plugin::ForwardOutput::AckHandler.new(timeout: 10, log: Fluent::Test::TestLogger.new, read_length: 100)
    node = flexmock('node', host: '127.0.0.1', port: '1000') # for log

    r1, w1 = IO.pipe
    r2, w2 = IO.pipe
    begin
      w1.write('readable')
      mock(r1).recv(anything) { |_| first_response } # IO does not have recv
      ack_handler.create_ack('chunk_id 111', node).enqueue(r1)
      first_results = []
      ack_handler.collect_response(1) { |_cid, _n, _s, ret| first_results << ret }
      assert_equal [first_result], first_results

      w2.write('readable')
      mock(r2).recv(anything) { |_| MessagePack.pack({ 'ack' => Base64.encode64('chunk_id 222') }) }
      ack_handler.create_ack('chunk_id 222', node).enqueue(r2)

      results = []
      ack_handler.collect_response(1) do |cid, _n, _s, ret|
        # This block is rescued by ack_handler so it needs to invoke assertion outside of this block
        results << [cid, ret]
      end

      assert_equal [['chunk_id 222', Fluent::Plugin::ForwardOutput::AckHandler::Result::SUCCESS]], results
    ensure
      [r1, w1, r2, w2].each { |io| io.close rescue nil }
    end
  end

  test 'an ack read on another thread is not lost' do
    fed, resume = Queue.new, Queue.new
    # Stop the first reader right after #feed, before #read.
    stub.proxy(Fluent::MessagePackFactory).msgpack_unpacker do |unpacker|
      stub.proxy(unpacker).feed do |ret|
        if Thread.current[:pause_after_feed]
          fed << true
          resume.pop
        end
        ret
      end
      unpacker
    end
    ack_handler = Fluent::Plugin::ForwardOutput::AckHandler.new(timeout: 10, log: Fluent::Test::TestLogger.new, read_length: 100)
    node = flexmock('node', host: '127.0.0.1', port: '1000') # for log

    r1, w1 = IO.pipe
    r2, w2 = IO.pipe
    begin
      w1.write('readable')
      mock(r1).recv(anything) { |_| r1.readpartial(100); MessagePack.pack({ 'ack' => Base64.encode64('chunk_id 111') }) } # IO does not have recv. Drain the pipe as a real recv would.
      ack_handler.create_ack('chunk_id 111', node).enqueue(r1)
      first_results = []
      reader = Thread.new do
        Thread.current[:pause_after_feed] = true
        ack_handler.collect_response(1) { |cid, _n, _s, ret| first_results << [cid, ret] }
      end
      assert_equal true, fed.pop(timeout: 5), 'first reader did not reach feed'

      w2.write('readable')
      mock(r2).recv(anything) { |_| r2.readpartial(100); MessagePack.pack({ 'ack' => Base64.encode64('chunk_id 222') }) }
      ack_handler.create_ack('chunk_id 222', node).enqueue(r2)
      second_results = []
      second_reader = Thread.new do
        ack_handler.collect_response(1) { |cid, _n, _s, ret| second_results << [cid, ret] }
      end
      assert_not_nil second_reader.join(5), 'second reader did not finish'
      resume << true
      assert_not_nil reader.join(5), 'first reader did not finish'

      assert_equal [[['chunk_id 111', Fluent::Plugin::ForwardOutput::AckHandler::Result::SUCCESS]],
                    [['chunk_id 222', Fluent::Plugin::ForwardOutput::AckHandler::Result::SUCCESS]]],
                   [first_results, second_results]
    ensure
      resume.close
      [reader, second_reader].compact.each { |t| t.kill.join(5) if t.alive? }
      [r1, w1, r2, w2].each { |io| io.close rescue nil }
    end
  end

  test 'when ack is expired' do
    ack_handler = Fluent::Plugin::ForwardOutput::AckHandler.new(timeout: 0, log: $log, read_length: 100)

    node = flexmock('node', host: '127.0.0.1', port: '1000') # for log
    chunk_id = 'chunk_id 111'
    ack = ack_handler.create_ack(chunk_id, node)

    r, w = IO.pipe
    begin
      w.write(chunk_id)
      mock(r).recv(anything).never
      ack.enqueue(r)

      a1 = a2 = a3 = a4 = nil
      ack_handler.collect_response(1) do |cid, n, s, ret|
        # This block is rescued by ack_handler so it needs to invoke assertion outside of this block
        a1 = cid; a2 = n; a3 = s; a4 = ret
      end

      assert_equal chunk_id, a1
      assert_equal node, a2
      assert_equal r, a3
      assert_equal Fluent::Plugin::ForwardOutput::AckHandler::Result::FAILED, a4
    ensure
      r.close rescue nil
      w.close rescue nil
    end
  end

  # ForwardOutput uses AckHandler in multiple threads, so we need to assume this case.
  # If exclusive control for this case is implemented, this test may not be necessary.
  test 'raises no error when another thread closes a socket' do
    ack_handler = Fluent::Plugin::ForwardOutput::AckHandler.new(timeout: 10, log: $log, read_length: 100)

    node = flexmock('node', host: '127.0.0.1', port: '1000') # for log
    chunk_id = 'chunk_id 111'
    ack = ack_handler.create_ack(chunk_id, node)

    r, w = IO.pipe
    begin
      w.write(chunk_id)
      def r.recv(arg)
        sleep(1) # To ensure that multiple threads select the socket before closing.
        raise IOError, 'stream closed in another thread' if self.closed?
        MessagePack.pack({ 'ack' => Base64.encode64('chunk_id 111') })
      end
      ack.enqueue(r)

      threads = []
      2.times do
        threads << Thread.new do
          ack_handler.collect_response(1) do |cid, n, s, ret|
            s&.close
          end
        end
      end

      assert_true threads.map{ |t| t.join(10) }.all?
      assert_false(
        $log.out.logs.any? { |log| log.include?('[error]') },
        $log.out.logs.select{ |log| log.include?('[error]') }.join('\n')
      )
    ensure
      r.close rescue nil
      w.close rescue nil
    end
  end
end
