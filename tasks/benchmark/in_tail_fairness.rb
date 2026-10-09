# Run with: bundle exec ruby -Ilib tasks/benchmark/in_tail_fairness.rb
# Synthetic scheduling benchmark: one long file competes with many short files.
# Sleep stands in for work that releases the GVL (for example blocking IO); it
# is not a parser benchmark and does not model real file IO or output plugins.

require 'cool.io'
require 'fluent/plugin/in_tail/worker_pool'

module TailFairnessBenchmark
  LARGE_BATCHES = Integer(ENV.fetch('LARGE_BATCHES', '80'))
  SHORT_FILES = Integer(ENV.fetch('SHORT_FILES', '20'))
  WORKERS = Integer(ENV.fetch('WORKERS', '4'))
  LARGE_WORK = Float(ENV.fetch('LARGE_WORK', '0.01'))
  SHORT_WORK = Float(ENV.fetch('SHORT_WORK', '0.002'))

  def self.measure_sequential
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    LARGE_BATCHES.times { sleep LARGE_WORK }
    short_latencies = Array.new(SHORT_FILES) do
      sleep SHORT_WORK
      Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    end
    [short_latencies, Process.clock_gettime(Process::CLOCK_MONOTONIC) - started]
  end

  def self.measure_workers
    loop = Coolio::Loop.new
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    short_latencies = []
    short_started = {}
    pending = Queue.new
    pending << [:large, 0]
    SHORT_FILES.times do |index|
      short_started[index] = started
      pending << [index, 0]
    end
    completed = 0
    total = LARGE_BATCHES + SHORT_FILES
    pool = nil
    watcher = Fluent::Plugin::TailInput::WorkerCompletionWatcher.new do
      while (result = pool.next_result)
        raise result.error if result.error

        key, batch = result.value
        if key == :large
          if batch + 1 < LARGE_BATCHES
            pending << [:large, batch + 1]
          end
        else
          short_latencies << Process.clock_gettime(Process::CLOCK_MONOTONIC) - short_started[key]
        end
        pool.acknowledge(result)
        completed += 1
      end

      while completed < total && pool.statistics[:tasks] < WORKERS && (job = pending.pop(true) rescue nil)
        key, batch = job
        raise 'scheduler could not submit available work' unless pool.submit(key, [key, batch], bytes: 1)
      end

      if completed == total
        loop.stop
      elsif pool.statistics[:tasks].zero? && pending.empty?
        raise 'scheduler stalled before all work completed'
      end
    end
    watcher.attach(loop)
    pool = Fluent::Plugin::TailInput::WorkerPool.new(
      num_threads: WORKERS, task_limit: WORKERS, byte_limit: WORKERS,
      thread_create: ->(title, &block) { Thread.new { Thread.current.name = title.to_s; block.call } },
      notify: watcher.method(:signal)
    ) do |_index, (key, batch)|
      sleep(key == :large ? LARGE_WORK : SHORT_WORK)
      [key, batch]
    end
    pool.start
    # Seed one task per worker, preserving the initial large-then-small order.
    WORKERS.times do
      key, batch = pending.pop
      pool.submit(key, [key, batch], bytes: 1)
    end

    Timeout.timeout(60) { loop.run }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    [short_latencies, elapsed]
  ensure
    pool&.stop
    pool&.join
    watcher&.close
  end

  def self.percentile(values, fraction)
    values.sort[[(values.size * fraction).ceil - 1, 0].max]
  end

  def self.report(name, result)
    latencies, elapsed = result
    puts format('%s,%.4f,%.4f,%.4f,%.4f', name, elapsed,
                percentile(latencies, 0.50), percentile(latencies, 0.95), latencies.max)
  end

  def self.main
    require 'timeout'
    unless LARGE_BATCHES > 0 && SHORT_FILES > 0 && WORKERS > 0 && WORKERS <= SHORT_FILES + 1 && LARGE_WORK >= 0 && SHORT_WORK >= 0
      raise 'work counts must be positive and work durations nonnegative'
    end

    puts RUBY_DESCRIPTION
    puts "#{LARGE_BATCHES} large-file batches, #{SHORT_FILES} short files, #{WORKERS} workers"
    puts format('mode,total_seconds,short_p50_seconds,short_p95_seconds,short_max_seconds')
    report('sequential', measure_sequential)
    report('workers', measure_workers)
  end
end

TailFairnessBenchmark.main
