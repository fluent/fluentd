require 'benchmark'
require 'fluent/plugin/in_tail'

class Fluent::Plugin::TailInput::TailWatcher::IOHandler
  alias_method :original_with_io, :with_io

  def with_io(&block)
    @benchmark_measured_in_tail ||= false
    # Measure the benchmark only once.
    return original_with_io(&block) if @benchmark_measured_in_tail

    Benchmark.bm do |x|
      x.report {
        original_with_io(&block)
        @benchmark_measured_in_tail = true
      }
    end

    exit 0
  end
end
