#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

module Fluent
  module PluginHelper
    module RetryState
      def retry_state_create(
          title, retry_type, wait, timeout,
          forever: false, max_steps: nil, backoff_base: 2, max_interval: nil, randomize: true, randomize_width: 0.125,
          secondary: false, secondary_threshold: 0.8
      )
        case retry_type
        when :exponential_backoff
          ExponentialBackOffRetry.new(title, wait, timeout, forever, max_steps, randomize, randomize_width, backoff_base, max_interval, secondary, secondary_threshold)
        when :periodic
          PeriodicRetry.new(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threshold)
        else
          raise "BUG: unknown retry_type specified: '#{retry_type}'"
        end
      end

      class RetryStateMachine
        attr_reader :title, :start, :steps, :next_time, :timeout_at, :current, :secondary_transition_at, :secondary_transition_steps

        def initialize(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threshold)
          @title = title

          @start = current_time
          @steps = 0
          @next_time = nil # should be initialized for first retry by child class

          @timeout = timeout
          @timeout_at = @start + timeout
          @has_reached_timeout = false
          @has_timed_out = false
          @current = :primary

          if randomize_width < 0 || randomize_width > 0.5
            raise "BUG: randomize_width MUST be between 0 and 0.5"
          end

          @randomize = randomize
          @randomize_width = randomize_width

          @forever = forever
          @max_steps = max_steps

          @secondary = secondary
          @secondary_threshold = secondary_threshold
          if @secondary
            raise "BUG: secondary_transition_threshold MUST be between 0 and 1" if @secondary_threshold <= 0 || @secondary_threshold >= 1
            max_retry_timeout = timeout
            if max_steps
              timeout_by_max_steps = calc_max_retry_timeout(max_steps)
              max_retry_timeout = timeout_by_max_steps if timeout_by_max_steps < max_retry_timeout
            end
            @secondary_transition_at = @start + max_retry_timeout * @secondary_threshold
            @secondary_transition_steps = nil
          end
        end

        def current_time
          Time.now
        end

        def randomize(interval)
          return interval unless @randomize

          interval + (interval * @randomize_width * (2 * rand - 1.0))
        end

        def calc_next_time
          if @forever || !@secondary # primary
            naive = naive_next_time(@steps)
            if @forever
              naive
            elsif naive >= @timeout_at
              @timeout_at
            else
              naive
            end
          elsif @current == :primary && @secondary
            naive = naive_next_time(@steps)
            if naive >= @secondary_transition_at
              @secondary_transition_at
            else
              naive
            end
          elsif @current == :secondary
            naive = naive_next_time(@steps - @secondary_transition_steps)
            if naive >= @timeout_at
              @timeout_at
            else
              naive
            end
          else
            raise "BUG: it's out of design"
          end
        end

        def naive_next_time(retry_times)
          raise NotImplementedError
        end

        def secondary?
          !@forever && @secondary && (@current == :secondary || current_time >= @secondary_transition_at)
        end

        def step
          @steps += 1
          if !@forever && @secondary && @current != :secondary && current_time >= @secondary_transition_at
            @current = :secondary
            @secondary_transition_steps = @steps
          end

          @next_time = calc_next_time

          if @has_reached_timeout
            @has_timed_out = @next_time >= @timeout_at
          else
            @has_reached_timeout = @next_time >= @timeout_at
          end

          nil
        end

        def recalc_next_time
          @next_time = calc_next_time
        end

        def limit?
          if @forever
            false
          else
            @has_timed_out || !!(@max_steps && @steps >= @max_steps)
          end
        end
      end

      class ExponentialBackOffRetry < RetryStateMachine
        def initialize(title, wait, timeout, forever, max_steps, randomize, randomize_width, backoff_base, max_interval, secondary, secondary_threshold)
          @constant_factor = wait
          @backoff_base = backoff_base
          @max_interval = max_interval

          super(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threshold)

          @next_time = @start + @constant_factor
        end

        def naive_next_time(retry_next_times)
          intr = calc_interval(retry_next_times)
          current_time + randomize(intr)
        end

        def calc_max_retry_timeout(max_steps)
          result = 0
          max_steps.times { |i|
            result += calc_interval(i)
          }
          result
        end

        def calc_interval(num)
          interval = raw_interval(num)
          if @max_interval && interval > @max_interval
            @max_interval
          else
            if interval.finite?
              interval
            else
              # Calculate previous finite value to avoid inf related errors. If this re-computing is heavy, use cache.
              until interval.finite?
                num -= 1
                interval = raw_interval(num)
              end
              interval
            end
          end
        end

        def raw_interval(num)
          @constant_factor.to_f * (@backoff_base ** (num))
        end
      end

      class PeriodicRetry < RetryStateMachine
        def initialize(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threshold)
          @retry_wait = wait

          super(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threshold)

          @next_time = @start + @retry_wait
        end

        def naive_next_time(retry_next_times)
          current_time + randomize(@retry_wait)
        end

        def calc_max_retry_timeout(max_steps)
          @retry_wait * max_steps
        end
      end
    end
  end
end
