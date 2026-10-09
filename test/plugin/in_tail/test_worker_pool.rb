require_relative '../../helper'
require 'fluent/plugin/in_tail/worker_pool'
require 'timeout'

class TailInputWorkerPoolTest < Test::Unit::TestCase
  def setup
    @pools = []
    @watchers = []
    @release = Queue.new
  end

  def teardown
    10.times { @release << true }
    @pools.each do |pool|
      pool.stop
      assert_true pool.join(timeout: 5)
    end
    @watchers.each(&:close)
    super
  end

  def create_pool(task_limit: 2, byte_limit: 10, num_threads: 1, &process)
    pool = Fluent::Plugin::TailInput::WorkerPool.new(
      num_threads: num_threads, task_limit: task_limit, byte_limit: byte_limit,
      thread_create: ->(_title, &block) { Thread.new(&block) },
      notify: -> {}, &process
    )
    @pools << pool
    pool.start
  end

  def result_from(pool)
    Timeout.timeout(5) do
      loop do
        result = pool.next_result
        return result if result
        Thread.pass
      end
    end
  end

  test 'file and capacity remain reserved until the owner acknowledges emit' do
    pool = create_pool(task_limit: 1) { |_, lines| lines.upcase }
    assert_true pool.submit(:file, 'first', bytes: 5)
    result = result_from(pool)

    assert_equal 'FIRST', result.value
    assert_false pool.submit(:file, 'second', bytes: 1)
    assert_false pool.submit(:other_file, 'other', bytes: 1)
    assert_equal({ tasks: 1, bytes: 5, state: :running }, pool.statistics)

    pool.acknowledge(result)
    assert_true pool.submit(:file, 'second', bytes: 1)
    assert_raise(ArgumentError) { pool.acknowledge(result) }
  end

  test 'worker compatibility requires every hook to resolve to the captured implementation' do
    base = Class.new do
      Fluent::Plugin::TailInput::WorkerCompatibility::HOOKS.each do |name|
        define_method(name) {}
      end
      private :io_handler
    end
    compatibility = Fluent::Plugin::TailInput::WorkerCompatibility.new(base)
    plugin = Class.new(base).new
    assert_true compatibility.compatible?(plugin)

    plugin_class = Class.new(base) { def receive_lines; end }
    assert_false compatibility.compatible?(plugin_class.new)

    private_override = Class.new(base) { private; def io_handler; end }
    assert_false compatibility.compatible?(private_override.new)

    prepended = Class.new(base)
    prepended.prepend(Module.new { def parse_singleline; end })
    assert_false compatibility.compatible?(prepended.new)

    singleton_override = Class.new(base).new
    def singleton_override.build_line_feeder; end
    assert_false compatibility.compatible?(singleton_override)
  end

  test 'byte capacity includes queued and completed batches' do
    pool = create_pool { |_, payload| @release.pop; payload }
    assert_true pool.submit(:first, 'first', bytes: 7)
    assert_false pool.submit(:second, 'second', bytes: 4)
    assert_true pool.submit(:second, 'second', bytes: 3)
    assert_false pool.submit(:third, 'third', bytes: 0)
    2.times { @release << true }

    first, second = result_from(pool), result_from(pool)
    assert_equal 10, pool.statistics[:bytes]
    pool.acknowledge(first)
    pool.acknowledge(second)
    assert_equal 0, pool.statistics[:bytes]
    assert_true pool.submit(:too_large, 'oversized', bytes: 11)
    @release << true
    pool.acknowledge(result_from(pool))
  end

  test 'one oversized batch uses a separate reservation beside regular capacity' do
    started = Queue.new
    pool = create_pool(task_limit: 3, byte_limit: 10, num_threads: 2) do |_, payload|
      started << payload
      @release.pop
      payload
    end
    assert_true pool.submit(:large, :oversized, bytes: 11)
    assert_equal :oversized, Timeout.timeout(5) { started.pop }
    assert_true pool.submit(:small, :regular, bytes: 10)
    assert_equal :regular, Timeout.timeout(5) { started.pop }
    assert_false pool.submit(:another_large, :oversized, bytes: 12)
    assert_false pool.submit(:another_small, :regular, bytes: 1)
    assert_equal({ tasks: 2, bytes: 21, state: :running }, pool.statistics)

    2.times { @release << true }
    results = 2.times.map { result_from(pool) }
    assert_equal %i[oversized regular], results.map(&:value).sort
    results.each { |result| pool.acknowledge(result) }
    assert_equal({ tasks: 0, bytes: 0, state: :running }, pool.statistics)
  end

  test 'a failed task produces an error result and the worker handles the next file' do
    pool = create_pool { |_, payload| raise Interrupt, 'task failed' if payload == :fail; payload }
    assert_true pool.submit(:file, :fail, bytes: 1)
    result = result_from(pool)
    assert_kind_of Interrupt, result.error
    pool.acknowledge(result)

    assert_true pool.submit(:file, :next, bytes: 1)
    result = result_from(pool)
    assert_nil result.error
    assert_equal :next, result.value
    pool.acknowledge(result)
  end

  test 'stop drains accepted work and rejects new batches' do
    pool = create_pool { |_, payload| @release.pop; payload }
    assert_true pool.submit(:first, 'first', bytes: 1)
    assert_true pool.submit(:second, 'second', bytes: 1)
    pool.stop
    assert_false pool.submit(:third, 'third', bytes: 1)
    assert_false pool.join(timeout: 0)

    2.times { @release << true }
    2.times { pool.acknowledge(result_from(pool)) }
    assert_true pool.join(timeout: 5)
    assert_equal 0, pool.statistics[:tasks]
  end

  test 'completion notifications never block when the pipe is full' do
    watcher = Fluent::Plugin::TailInput::WorkerCompletionWatcher.new {}
    @watchers << watcher
    Timeout.timeout(5) do
      loop { break unless watcher.signal }
    end
    assert_false watcher.signal
  end

  test 'different files run concurrently while each file stays reserved' do
    large_started = Queue.new
    large_finished = Queue.new
    short_completed = Queue.new
    pool = create_pool(task_limit: 3, num_threads: 2) do |_, payload|
      if payload == :large
        large_started << true
        @release.pop
        large_finished << true
      else
        short_completed << payload
      end
      payload
    end
    assert_true pool.submit(:large_file, :large, bytes: 1)
    Timeout.timeout(5) { large_started.pop }
    assert_true pool.submit(:short_file_a, :short_a, bytes: 1)
    assert_true pool.submit(:short_file_b, :short_b, bytes: 1)
    short_records = Timeout.timeout(5) { [short_completed.pop, short_completed.pop] }

    assert_equal [:short_a, :short_b], short_records.sort
    assert_true large_finished.empty?
    assert_equal 3, pool.statistics[:tasks]
    assert_false pool.submit(:large_file, :later_batch, bytes: 1)

    @release << true
    results = 3.times.map { result_from(pool) }
    assert_equal [:large, :short_a, :short_b], results.map(&:value).sort_by(&:to_s)
    results.each { |result| pool.acknowledge(result) }
  end

  test 'repeated start does not stop the running pool' do
    pool = create_pool { |_, payload| payload }
    assert_raise(RuntimeError) { pool.start }
    assert_true pool.submit(:file, 'lines', bytes: 1)
    pool.acknowledge(result_from(pool))
  end

  test 'failed thread creation cleans up workers already created' do
    threads = []
    pool = Fluent::Plugin::TailInput::WorkerPool.new(
      num_threads: 2, task_limit: 2, byte_limit: 10,
      thread_create: ->(_title, &block) {
        raise 'failed to start' unless threads.empty?
        threads << Thread.new(&block)
        threads.last
      }, notify: -> {}
    ) { |_, payload| payload }
    @pools << pool
    assert_raise(RuntimeError) { pool.start }
    assert_true threads.all? { |thread| !thread.alive? }
    assert_false pool.submit(:file, 'lines', bytes: 1)
  end

  test 'unexpected worker termination reports active and queued work as failed' do
    threads = Queue.new
    pool = create_pool do |_, payload|
      threads << Thread.current
      @release.pop
      payload
    end
    pool.submit(:active, 'active', bytes: 1)
    pool.submit(:queued, 'queued', bytes: 1)
    worker = Timeout.timeout(5) { threads.pop }
    worker.kill
    worker.join
    results = [result_from(pool), result_from(pool)]

    assert_true results.all? { |result| result.error.is_a?(Fluent::Plugin::TailInput::WorkerPool::WorkerStopped) }
    assert_equal :failed, pool.statistics[:state]
    assert_not_nil pool.failure
    assert_false pool.submit(:next, 'next', bytes: 1)
    results.each { |result| pool.acknowledge(result) }
  end

  test 'notification failure retains results and shuts down admissions' do
    pool = Fluent::Plugin::TailInput::WorkerPool.new(
      num_threads: 1, task_limit: 1, byte_limit: 10,
      thread_create: ->(_title, &block) { Thread.new(&block) },
      notify: -> { raise IOError, 'notification channel closed' }
    ) { |_, payload| payload }
    @pools << pool
    pool.start
    pool.submit(:file, 'lines', bytes: 1)
    result = result_from(pool)
    assert_true pool.join(timeout: 5)
    assert_kind_of IOError, pool.failure
    assert_equal 'lines', result.value
    assert_false pool.submit(:other, 'other', bytes: 1)
    pool.acknowledge(result)
  end

  test 'a worker completion wakes the event loop on its own thread' do
    loop = Coolio::Loop.new
    result = nil
    callback_thread = nil
    pool = nil
    watcher = Fluent::Plugin::TailInput::WorkerCompletionWatcher.new do
      result = pool.next_result
      callback_thread = Thread.current
      loop.stop
    end
    @watchers << watcher
    watcher.attach(loop)
    pool = Fluent::Plugin::TailInput::WorkerPool.new(
      num_threads: 1, task_limit: 1, byte_limit: 10,
      thread_create: ->(_title, &block) { Thread.new(&block) },
      notify: watcher.method(:signal)
    ) { |_, payload| [Thread.current, payload] }
    @pools << pool
    pool.start
    pool.submit(:file, 'lines', bytes: 5)
    Timeout.timeout(5) { loop.run }

    assert_equal Thread.current, callback_thread
    assert_not_equal callback_thread, result.value[0]
    assert_equal 'lines', result.value[1]
    pool.acknowledge(result)
  end
end
