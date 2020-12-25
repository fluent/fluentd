require_relative '../../helper'
require 'fluent/plugin/in_tail'

class TailThreadPoolTest < Test::Unit::TestCase
  data("single thread" => 1,
       "max thread pool size (10)" => 10,
       "max thread pool size (20)" => 20,
      )
  test "thread pool creation" do |data|
    max_threads_pool_size = data
    thread_pool = Fluent::Plugin::TailInput::TailThread::Pool.new(max_threads_pool_size) do |pool|
      100.times {|n|
        pool.run {
          nop_task(0.01)
          assert do
            pool.instance_variable_get(:@threads).size <= max_threads_pool_size
          end
        }
      }
    end
  end

  def nop_task(sleep_time)
    sleep sleep_time
  end
end
