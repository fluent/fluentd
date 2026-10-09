# Run with: bundle exec ruby -Ilib tasks/benchmark/in_tail_fairness.rb
# Generates real JSONL files and measures how late the event loop dispatches a
# second-file update while a large file is being drained. This targets watcher
# latency, not total throughput; it isolates read/parse scheduling from output.

require 'cool.io'
require 'json'
require 'tmpdir'
require 'timeout'
require 'fluent/plugin/in_tail/worker_pool'

module TailFairnessBenchmark
  LARGE_MB = Integer(ENV.fetch('LARGE_MB', '100'))
  SHORT_MB = Integer(ENV.fetch('SHORT_MB', '1'))
  WORKERS = Integer(ENV.fetch('WORKERS', '4'))
  BATCH_LINES = Integer(ENV.fetch('BATCH_LINES', '1000'))
  REPEATS = Integer(ENV.fetch('REPEATS', '3'))
  UPDATE_AFTER = Float(ENV.fetch('UPDATE_AFTER', '0.05'))
  LINE = JSON.generate('message' => 'x' * 80) + "\n"

  class OneShotTimer < Coolio::TimerWatcher
    def initialize(delay, &callback)
      @callback = callback
      super(delay, false)
    end

    def on_timer
      @callback.call
    end
  end

  def self.create_file(path, megabytes)
    target_bytes = megabytes * 1024 * 1024
    block = LINE * 4096
    File.open(path, 'wb') do |file|
      written = 0
      while written < target_bytes
        file.write(block)
        written += block.bytesize
      end
    end
  end

  def self.read_batch(io)
    lines = []
    BATCH_LINES.times do
      line = io.gets
      break unless line
      lines << line
    end
    lines.empty? ? nil : lines
  end

  def self.parse(lines)
    lines.each { |line| JSON.parse(line).fetch('message') }
    lines.size
  end

  def self.measure(mode, large_path, short_path)
    loop = Coolio::Loop.new
    large_io = File.open(large_path, 'rb')
    short_io = File.open(short_path, 'rb')
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    update_due = started + UPDATE_AFTER
    dispatch_latency = nil
    first_short_latency = nil
    large_records = 0
    short_records = 0
    pool = nil
    completion_watcher = nil
    submit_large = nil
    submit_short = nil

    update_timer = OneShotTimer.new(UPDATE_AFTER) do
      dispatch_latency = Process.clock_gettime(Process::CLOCK_MONOTONIC) - update_due
      lines = read_batch(short_io)
      if mode == :sync
        short_records += parse(lines) if lines
        first_short_latency = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        loop.stop
      elsif lines
        submit_short.call(lines)
      else
        first_short_latency = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
        loop.stop
      end
    end
    update_timer.attach(loop)

    kickoff = OneShotTimer.new(0.001) do
      if mode == :sync
        while (lines = read_batch(large_io))
          large_records += parse(lines)
        end
      else
        submit_large.call
      end
    end
    kickoff.attach(loop)

    if mode == :workers
      completion_watcher = Fluent::Plugin::TailInput::WorkerCompletionWatcher.new do
        while (result = pool.next_result)
          raise result.error if result.error
          if result.task.key == :large
            large_records += result.value
            pool.acknowledge(result)
            submit_large.call
          else
            short_records += result.value
            pool.acknowledge(result)
            first_short_latency = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
            loop.stop if loop.instance_variable_get(:@running)
          end
        end
      end
      completion_watcher.attach(loop)
      pool = Fluent::Plugin::TailInput::WorkerPool.new(
        num_threads: WORKERS, task_limit: WORKERS, byte_limit: WORKERS * BATCH_LINES * LINE.bytesize,
        thread_create: ->(title, &block) { Thread.new { Thread.current.name = title.to_s; block.call } },
        notify: completion_watcher.method(:signal)
      ) { |_worker, lines| parse(lines) }
      pool.start
      submit_large = lambda do
        lines = read_batch(large_io)
        if lines
          raise 'could not enqueue large-file batch' unless pool.submit(:large, lines, bytes: lines.sum(&:bytesize))
        end
      end
      submit_short = lambda do |lines|
        raise 'could not enqueue short-file batch' unless pool.submit(:short, lines, bytes: lines.sum(&:bytesize))
      end
    end

    Timeout.timeout(600) { loop.run }
    {
      dispatch: dispatch_latency,
      first_short: first_short_latency,
      large_records: large_records,
      short_records: short_records,
      elapsed: Process.clock_gettime(Process::CLOCK_MONOTONIC) - started,
    }
  ensure
    pool&.stop
    pool&.join
    completion_watcher&.close
    large_io&.close unless large_io&.closed?
    short_io&.close unless short_io&.closed?
  end

  def self.main
    unless LARGE_MB.positive? && SHORT_MB.positive? && WORKERS.positive? && BATCH_LINES.positive? &&
        REPEATS.positive? && UPDATE_AFTER.positive?
      raise 'file sizes, worker count, batch size, and update delay must be positive'
    end

    Dir.mktmpdir('in-tail-fairness') do |directory|
      large_path = File.join(directory, 'large.jsonl')
      short_path = File.join(directory, 'updated.jsonl')
      puts "Generating #{LARGE_MB} MiB large file and #{SHORT_MB} MiB updated file..."
      create_file(large_path, LARGE_MB)
      create_file(short_path, SHORT_MB)
      puts RUBY_DESCRIPTION
      puts "large=#{LARGE_MB}MiB, updated=#{SHORT_MB}MiB, workers=#{WORKERS}, batch_lines=#{BATCH_LINES}, update_after=#{UPDATE_AFTER}s, repeats=#{REPEATS}"
      puts 'mode,update_dispatch_lag_seconds,first_short_batch_seconds,large_records_before_update,elapsed_seconds'

      samples = { sync: [], workers: [] }
      REPEATS.times do |index|
        (index.even? ? %i[sync workers] : %i[workers sync]).each do |mode|
          samples[mode] << measure(mode, large_path, short_path)
        end
      end
      samples.each do |mode, results|
        median = lambda do |key|
          results.map { |result| result[key] }.sort[REPEATS / 2]
        end
        puts format('%s,%.4f,%.4f,%d,%.4f', mode, median.call(:dispatch), median.call(:first_short),
                    median.call(:large_records), median.call(:elapsed))
      end
    end
  end
end

TailFairnessBenchmark.main
