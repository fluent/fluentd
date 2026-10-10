# Run with: bundle exec ruby -Ilib tasks/benchmark/in_tail_parse_threads.rb
# MODE=feed also offloads emit. EMIT_WAIT adds a synthetic blocking collector.
# This does not measure file IO or real downstream plugins.

require 'json'
require 'stringio'
require 'serverengine'
require 'fluent/engine'
require 'fluent/log'
require 'fluent/plugin/in_tail/line_feeder'
require 'fluent/plugin/in_tail/worker_pool'
require 'fluent/plugin/parser_none'
require 'fluent/plugin/parser_json'
require 'fluent/plugin/parser_regexp'

$log = Fluent::Log.new(ServerEngine::DaemonLogger.new(StringIO.new))
Fluent::Engine.init(Fluent::SystemConfig.new)

module TailParseThreadsBenchmark
  Watcher = Struct.new(:path, :tag, :file_feed)
  BATCH_SIZE = 1000
  BATCHES = Integer(ENV.fetch('BATCHES', '200'))
  REPEATS = Integer(ENV.fetch('REPEATS', '3'))
  FILES = Integer(ENV.fetch('FILES', '4'))
  MODE = ENV.fetch('MODE', 'parse')
  EMIT_WAIT = Float(ENV.fetch('EMIT_WAIT', '0'))

  class Router
    attr_reader :count

    def initialize
      @count = 0
      @thread = Thread.current
      @mutex = Mutex.new
    end

    def emit_stream(tag, events)
      if MODE == 'parse' && Thread.current != @thread
        raise 'emit moved off the caller thread'
      end

      sleep EMIT_WAIT if EMIT_WAIT > 0
      @mutex.synchronize { @count += events.size }
    end
  end

  def self.create_feeder(type, router)
    params = { '@type' => type }
    params['expression'] = '^(?<message>.+)$' if type == 'regexp'
    parser = Fluent::Plugin.new_parser(type)
    parser.configure(Fluent::Config::Element.new('parse', '', params, []))
    parser.start
    parser.after_start
    feeder = Fluent::Plugin::TailInput::LineFeeder.new(
      parser: parser, router_provider: -> { router }, log: $log,
      tag: 'tail', tag_prefix: nil, tag_suffix: nil, path_key: nil,
      emit_unmatched_lines: false, multiline_mode: false
    )
    [feeder, parser]
  end

  def self.close_parser(parser)
    %i[stop before_shutdown shutdown after_shutdown close terminate].each do |method|
      parser.public_send(method)
    end
  end

  def self.run(type, line, num_threads)
    router = Router.new
    resources = Array.new(num_threads) { create_feeder(type, router) }
    feeders = resources.map(&:first)
    batches = Array.new(BATCHES) { Array.new(BATCH_SIZE) { line.dup } }
    file_batches = Array.new(FILES) { [] }
    batches.each_with_index { |batch, index| file_batches[index % FILES] << batch }
    watchers = Array.new(FILES) do |index|
      Watcher.new("benchmark-#{index}.log", "benchmark.#{index}", feeders.first.new_file_feed)
    end
    ready = Queue.new
    notifications = Queue.new
    pool = nil

    if num_threads > 1
      pool = Fluent::Plugin::TailInput::WorkerPool.new(
        num_threads: num_threads, task_limit: num_threads * 2,
        byte_limit: num_threads * 2 * BATCH_SIZE * line.bytesize,
        thread_create: ->(_title, &block) {
          Thread.new { ready << true; block.call }
        }, notify: -> { notifications << true }
      ) do |index, payload|
        file, batch = payload
        if MODE == 'parse'
          feeders[index].parse(batch, watchers[file])
        else
          feeders[index].feed_lines(batch, watchers[file])
        end
      end
      pool.start
      num_threads.times { ready.pop }
    end

    GC.start
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    if num_threads == 1
      file_batches.each_with_index do |inputs, file|
        inputs.each { |batch| feeders.first.feed_lines(batch, watchers[file]) }
      end
    else
      completed = 0
      busy = Array.new(FILES, false)
      positions = Array.new(FILES, 0)
      while completed < batches.size
        FILES.times do |file|
          next if busy[file] || positions[file] >= file_batches[file].size

          batch = file_batches[file][positions[file]]
          if pool.submit(file, [file, batch], bytes: batch.sum(&:bytesize))
            positions[file] += 1
            busy[file] = true
          end
        end
        completion = pool.next_result
        unless completion
          notifications.pop
          next
        end
        file = completion.task.key
        result = completion.value
        raise completion.error if completion.error
        raise 'completion without an active file' unless busy[file]

        if MODE == 'parse'
          feeders.first.emit(result, watchers[file])
        else
          raise 'emit failed' unless result
        end
        pool.acknowledge(completion)
        busy[file] = false
        completed += 1
      end
    end
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    raise "missing events: #{router.count}" unless router.count == BATCHES * BATCH_SIZE

    elapsed
  ensure
    pool&.stop
    pool&.join
    resources&.each { |_, parser| close_parser(parser) }
  end

  def self.main
    raise 'MODE must be parse or feed' unless %w[parse feed].include?(MODE)
    unless BATCHES > 0 && REPEATS > 0 && FILES > 0
      raise 'BATCHES, REPEATS and FILES must be positive'
    end
    raise 'EMIT_WAIT must be nonnegative' if EMIT_WAIT < 0

    workloads = {
      'none' => "#{'message ' * 32}\n",
      'regexp' => "#{'message ' * 32}\n",
      'json' => "#{JSON.generate('time' => 1234567890, 'message' => 'message ' * 32, 'values' => (1..32).to_a)}\n",
    }
    puts RUBY_DESCRIPTION
    puts "#{FILES} files, #{BATCHES} batches x #{BATCH_SIZE} lines, #{REPEATS} repeats (median)"
    puts "mode=#{MODE}, synthetic collector wait=#{EMIT_WAIT}s/batch"
    puts 'parser,threads,seconds,records_per_second,speedup'
    workloads.each do |type, line|
      samples = { 1 => [], 4 => [] }
      REPEATS.times do |index|
        # Alternate run order to reduce warm-up/order bias.
        (index.even? ? [1, 4] : [4, 1]).each do |threads|
          samples[threads] << run(type, line, threads)
        end
      end
      baseline = samples[1].sort[REPEATS / 2]
      samples.each do |threads, times|
        elapsed = times.sort[REPEATS / 2]
        puts format('%s,%d,%.4f,%.0f,%.3f', type, threads, elapsed,
                    BATCHES * BATCH_SIZE / elapsed, baseline / elapsed)
      end
    end
  end
end

TailParseThreadsBenchmark.main
