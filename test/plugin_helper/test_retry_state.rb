require_relative '../helper'
require 'fluent/plugin_helper/retry_state'
require 'fluent/plugin/base'

require 'time'

class RetryStateHelperTest < Test::Unit::TestCase
  def override_current_time(state, time)
    mojule = Module.new do
      define_method(:current_time){ time }
    end
    state.singleton_class.module_eval do
      prepend mojule
    end
  end

  class Dummy < Fluent::Plugin::TestBase
    helpers :retry_state
  end

  setup do
    @d = Dummy.new
  end

  test 'randomize can generate value within specified +/- range' do
    s = @d.retry_state_create(:t1, :exponential_backoff, 0.1, 30) # default enabled w/ 0.125
    500.times do
      r = s.randomize(1000)
      assert{ r >= 875 && r < 1125 }
    end

    s = @d.retry_state_create(:t1, :exponential_backoff, 0.1, 30, randomize_width: 0.25)
    500.times do
      r = s.randomize(1000)
      assert{ r >= 750 && r < 1250 }
    end
  end

  test 'plugin can create retry_state machine' do
    s = @d.retry_state_create(:t1, :exponential_backoff, 0.1, 30)
    # attr_reader :title, :start, :steps, :next_time, :timeout_at, :current, :secondary_transition_at, :secondary_transition_times

    assert_equal :t1, s.title
    start_time = s.start

    assert_equal 0, s.steps
    assert_equal (start_time + 0.1).to_i, s.next_time.to_i
    assert_equal (start_time + 0.1).nsec, s.next_time.nsec
    assert_equal (start_time + 30), s.timeout_at

    assert_equal :primary, s.current
    assert{ s.is_a? Fluent::PluginHelper::RetryState::ExponentialBackOffRetry }
  end

  test 'periodic retries' do
    s = @d.retry_state_create(:t2, :periodic, 3, 29, randomize: false)
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time
    assert_equal (dummy_current_time + 29), s.timeout_at
    assert_equal (dummy_current_time + 3), s.next_time

    i = 1
    while i < 9
      override_current_time(s, s.next_time)
      s.step
      assert_equal i, s.steps
      assert_equal (s.current_time + 3), s.next_time
      assert !s.limit?
      i += 1
    end

    assert_equal 9, i
    override_current_time(s, s.next_time)
    s.step
    assert_equal s.timeout_at, s.next_time
    assert s.limit?
  end

  test 'periodic retries with max_steps' do
    s = @d.retry_state_create(:t2, :periodic, 3, 29, randomize: false, max_steps: 5)
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time
    assert_equal (dummy_current_time + 29), s.timeout_at
    assert_equal (dummy_current_time + 3), s.next_time

    i = 1
    while i < 5
      override_current_time(s, s.next_time)
      s.step
      assert_equal i, s.steps
      assert_equal (s.current_time + 3), s.next_time
      assert !s.limit?
      i += 1
    end

    assert_equal 5, i
    override_current_time(s, s.next_time)
    s.step
    assert_equal (s.current_time + 3), s.next_time
    assert s.limit?
  end

  test 'periodic retries with secondary' do
    s = @d.retry_state_create(:t3, :periodic, 3, 100, randomize: false, secondary: true) # threshold 0.8
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time
    assert_equal (dummy_current_time + 100), s.timeout_at
    assert_equal (dummy_current_time + 100 * 0.8), s.secondary_transition_at

    assert_equal (dummy_current_time + 3), s.next_time
    assert !s.secondary?

    i = 1
    while i < 26
      override_current_time(s, s.next_time)
      assert !s.secondary?

      s.step
      assert_equal i, s.steps
      assert_equal (s.current_time + 3), s.next_time
      assert !s.limit?
      i += 1
    end

    assert_equal 26, i
    override_current_time(s, s.next_time) # 78
    assert !s.secondary?

    s.step
    assert_equal 26, s.steps
    assert_equal s.secondary_transition_at, s.next_time
    assert !s.limit?

    i += 1
    assert_equal 27, i
    override_current_time(s, s.next_time) # 80
    assert s.secondary?

    s.step
    assert_equal (s.current_time + 3), s.next_time
    assert_equal s.steps, s.secondary_transition_steps
    assert !s.limit?

    i += 1

    while i < 33
      override_current_time(s, s.next_time)
      assert s.secondary?

      s.step
      assert_equal (s.current_time + 3), s.next_time
      assert !s.limit?
      i += 1
    end

    assert_equal 33, i
    override_current_time(s, s.next_time) # 98
    assert s.secondary?

    s.step
    assert_equal s.timeout_at, s.next_time
    assert s.limit?
  end

  test 'periodic retries with secondary and specified threshold' do
    s = @d.retry_state_create(:t3, :periodic, 3, 100, randomize: false, secondary: true, secondary_threshold: 0.75)
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time
    assert_equal (dummy_current_time + 100), s.timeout_at
    assert_equal (dummy_current_time + 100 * 0.75), s.secondary_transition_at
  end

  test 'periodic retries with secondary and max_steps' do
    s = @d.retry_state_create(:t3, :periodic, 3, 100, max_steps: 5, randomize: false, secondary: true)
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time
    assert_equal (dummy_current_time + 100), s.timeout_at
    assert_equal (dummy_current_time + 3 * 5 * 0.8), s.secondary_transition_at
  end

  test 'exponential backoff forever without randomization' do
    s = @d.retry_state_create(:t11, :exponential_backoff, 0.1, 300, randomize: false, forever: true, backoff_base: 2)
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time

    assert_equal 0, s.steps
    assert_equal (dummy_current_time + 0.1), s.next_time

    i = 1
    while i < 300
      s.step
      assert_equal i, s.steps
      assert_equal (dummy_current_time + 0.1 * (2 ** (i - 1))), s.next_time
      assert !s.limit?
      i += 1
    end
  end

  test 'exponential backoff with max_interval' do
    s = @d.retry_state_create(:t12, :exponential_backoff, 0.1, 300, randomize: false, forever: true, backoff_base: 2, max_interval: 100)
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time

    assert_equal 0, s.steps
    assert_equal (dummy_current_time + 0.1), s.next_time

    # 0.1 * (2 ** (10 - 1)) == 0.1 * 2 ** 9 == 51.2
    # 0.1 * (2 ** (11 - 1)) == 0.1 * 2 ** 10 == 102.4
    i = 1
    while i < 11
      s.step
      assert_equal i, s.steps
      assert_equal (dummy_current_time + 0.1 * (2 ** (i - 1))), s.next_time, "start:#{dummy_current_time}, i:#{i}"
      i += 1
    end

    s.step
    assert_equal 11, s.steps
    assert_equal (dummy_current_time + 100), s.next_time

    s.step
    assert_equal 12, s.steps
    assert_equal (dummy_current_time + 100), s.next_time
  end

  test 'exponential backoff with shorter timeout' do
    s = @d.retry_state_create(:t13, :exponential_backoff, 1, 12, randomize: false, backoff_base: 2, max_interval: 10)
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time

    assert_equal (dummy_current_time + 12), s.timeout_at

    assert_equal 0, s.steps
    assert_equal (dummy_current_time + 1), s.next_time

    # 1 + 1 + 2 + 4 (=8)

    override_current_time(s, s.next_time)
    s.step
    assert_equal 1, s.steps
    assert_equal (s.current_time + 1), s.next_time

    override_current_time(s, s.next_time)
    s.step
    assert_equal 2, s.steps
    assert_equal (s.current_time + 2), s.next_time

    override_current_time(s, s.next_time)
    s.step
    assert_equal 3, s.steps
    assert_equal (s.current_time + 4), s.next_time

    assert !s.limit?

    # + 8 (=16) > 12

    override_current_time(s, s.next_time)
    s.step
    assert_equal 4, s.steps
    assert_equal s.timeout_at, s.next_time

    assert s.limit?
  end

  test 'exponential backoff with max_steps' do
    s = @d.retry_state_create(:t14, :exponential_backoff, 1, 120, randomize: false, backoff_base: 2, max_interval: 10, max_steps: 6)
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time

    assert_equal (dummy_current_time + 120), s.timeout_at

    assert_equal 0, s.steps
    assert_equal (dummy_current_time + 1), s.next_time

    override_current_time(s, s.next_time)
    s.step
    assert_equal 1, s.steps
    assert_equal (s.current_time + 1), s.next_time

    override_current_time(s, s.next_time)
    s.step
    assert_equal 2, s.steps
    assert_equal (s.current_time + 2), s.next_time

    override_current_time(s, s.next_time)
    s.step
    assert_equal 3, s.steps
    assert_equal (s.current_time + 4), s.next_time

    assert !s.limit?

    override_current_time(s, s.next_time)
    s.step
    assert_equal 4, s.steps
    assert_equal (s.current_time + 8), s.next_time

    assert !s.limit?

    override_current_time(s, s.next_time)
    s.step
    assert_equal 5, s.steps
    assert_equal (s.current_time + 10), s.next_time

    assert !s.limit?

    override_current_time(s, s.next_time)
    s.step
    assert_equal 6, s.steps
    assert_equal (s.current_time + 10), s.next_time

    assert s.limit?
  end

  test 'exponential backoff retries with secondary' do
    s = @d.retry_state_create(:t15, :exponential_backoff, 1, 100, randomize: false, backoff_base: 2, secondary: true) # threshold 0.8
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time
    assert_equal (dummy_current_time + 100), s.timeout_at
    assert_equal (dummy_current_time + 100 * 0.8), s.secondary_transition_at

    assert_equal (dummy_current_time + 1), s.next_time
    assert !s.secondary?

    # 1, 1(2), 2(4), 4(8), 8(16), 16(32), 32(64), (80), (81), (83), (87), (95), (100)
    i = 1
    while i < 7
      override_current_time(s, s.next_time)
      assert !s.secondary?

      s.step
      assert_equal i, s.steps
      assert_equal (s.current_time + 1 * (2 ** (i - 1))), s.next_time
      assert !s.limit?
      i += 1
    end

    assert_equal 7, i
    override_current_time(s, s.next_time) # 64
    assert !s.secondary?

    s.step
    assert_equal 7, s.steps
    assert_equal s.secondary_transition_at, s.next_time
    assert !s.limit?

    i += 1
    assert_equal 8, i
    override_current_time(s, s.next_time) # 80
    assert s.secondary?

    s.step
    assert_equal 8, s.steps
    assert_equal s.steps, s.secondary_transition_steps
    assert_equal (s.secondary_transition_at + 1.0), s.next_time
    assert !s.limit?

    # 81, 82, 84, 88, 96, 100
    j = 1
    while j < 4
      override_current_time(s, s.next_time)
      assert s.secondary?
      assert_equal :secondary, s.current

      s.step
      assert_equal (8 + j), s.steps
      assert_equal (s.current_time + (1 * (2 ** j))), s.next_time
      assert !s.limit?, "j:#{j}"
      j += 1
    end

    assert_equal 4, j
    override_current_time(s, s.next_time) # 96
    assert s.secondary?

    s.step
    assert_equal s.timeout_at, s.next_time
    assert s.limit?
  end

  test 'exponential backoff retries with secondary and specified threshold' do
    s = @d.retry_state_create(:t16, :exponential_backoff, 1, 100, randomize: false, secondary: true, backoff_base: 2, secondary_threshold: 0.75)
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    assert_equal dummy_current_time, s.current_time
    assert_equal (dummy_current_time + 100), s.timeout_at
    assert_equal (dummy_current_time + 100 * 0.75), s.secondary_transition_at
  end

  test 'exponential backoff retries with secondary and max_steps' do
    s = @d.retry_state_create(:t15, :exponential_backoff, 1, 100, randomize: false, max_steps: 5, backoff_base: 2, secondary: true) # threshold 0.8
    dummy_current_time = s.start
    override_current_time(s, dummy_current_time)

    timeout = 0
    5.times { |i| timeout += 1.0 * (2 ** (i - 1)) }

    assert_equal dummy_current_time, s.current_time
    assert_equal (dummy_current_time + 100), s.timeout_at
    assert_equal (dummy_current_time + timeout * 0.8), s.secondary_transition_at
  end

  sub_test_case 'exponential backoff' do
    test 'too big steps(check inf handling)' do
      s = @d.retry_state_create(:t11, :exponential_backoff, 1, 300, randomize: false, forever: true, backoff_base: 2)
      dummy_current_time = s.start
      override_current_time(s, dummy_current_time)

      i = 1
      while i < 1027
        if i >= 1025
          # With this setting, 1025+ number causes inf in `calc_interval`, so 1024 value is used for next_time
          assert_nothing_raised(FloatDomainError) { s.step }
          assert_equal (dummy_current_time + (2 ** (1024 - 1))), s.next_time
        else
          s.step
        end
        i += 1
      end
    end
  end
end
