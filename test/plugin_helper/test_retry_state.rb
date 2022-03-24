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

  class RetryRecord
    attr_reader :retry_count, :elapsed_sec, :is_secondary
    def initialize(retry_count, elapsed_sec, is_secondary)
      @retry_count = retry_count # This is Nth retryment
      @elapsed_sec = elapsed_sec
      @is_secondary = is_secondary
    end

    def ==(obj)
      @retry_count == obj.retry_count &&
      @elapsed_sec == obj.elapsed_sec &&
      @is_secondary == obj.is_secondary
    end
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
    s.step
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
    assert_equal s.timeout_at, s.next_time # 100

    s.step
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
      assert_equal (dummy_current_time + 0.1 * (2 ** i)), s.next_time
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

    # 0.1 * 2 ** 9 == 51.2
    # 0.1 * 2 ** 10 == 102.4
    i = 1
    while i < 10
      s.step
      assert_equal i, s.steps
      assert_equal (dummy_current_time + 0.1 * (2 ** i)), s.next_time, "start:#{dummy_current_time}, i:#{i}"
      i += 1
    end

    s.step
    assert_equal 10, s.steps
    assert_equal (dummy_current_time + 100), s.next_time

    s.step
    assert_equal 11, s.steps
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

    # 1 + 2 + 4 (=7)

    override_current_time(s, s.next_time)
    s.step
    assert_equal 1, s.steps
    assert_equal (s.current_time + 2), s.next_time

    override_current_time(s, s.next_time)
    s.step
    assert_equal 2, s.steps
    assert_equal (s.current_time + 4), s.next_time

    assert !s.limit?

    # + 8 (=15) > 12

    override_current_time(s, s.next_time)
    s.step
    assert_equal 3, s.steps
    assert_equal s.timeout_at, s.next_time

    s.step
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
    assert_equal (s.current_time + 2), s.next_time

    override_current_time(s, s.next_time)
    s.step
    assert_equal 2, s.steps
    assert_equal (s.current_time + 4), s.next_time

    override_current_time(s, s.next_time)
    s.step
    assert_equal 3, s.steps
    assert_equal (s.current_time + 8), s.next_time

    assert !s.limit?

    override_current_time(s, s.next_time)
    s.step
    assert_equal 4, s.steps
    assert_equal (s.current_time + 10), s.next_time

    assert !s.limit?

    override_current_time(s, s.next_time)
    s.step
    assert_equal 5, s.steps
    assert_equal (s.current_time + 10), s.next_time

    assert !s.limit?

    override_current_time(s, s.next_time)
    s.step
    assert_equal 6, s.steps
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

    # primary: 3, 7, 15, 31, 63, 80 (timeout * threashold)
    # secondary: 81, 83, 87, 95, 100
    i = 1
    while i < 6
      override_current_time(s, s.next_time)
      assert !s.secondary?

      s.step
      assert_equal i, s.steps
      assert_equal (s.current_time + 1 * (2 ** i)), s.next_time
      assert !s.limit?
      i += 1
    end

    assert_equal 6, i
    override_current_time(s, s.next_time) # 63
    assert !s.secondary?

    s.step
    assert_equal 6, s.steps
    assert_equal s.secondary_transition_at, s.next_time
    assert !s.limit?

    i += 1
    assert_equal 7, i
    override_current_time(s, s.next_time) # 80
    assert s.secondary?

    s.step
    assert_equal 7, s.steps
    assert_equal s.steps, s.secondary_transition_steps
    assert_equal (s.secondary_transition_at + 1.0), s.next_time # 81
    assert !s.limit?
    assert_equal :secondary, s.current

    # 83, 87, 95, 100
    j = 1
    while j < 4
      override_current_time(s, s.next_time)
      assert s.secondary?
      assert_equal :secondary, s.current

      s.step
      assert_equal (7 + j), s.steps
      assert_equal (s.current_time + (1 * (2 ** j))), s.next_time
      assert !s.limit?, "j:#{j}"
      j += 1
    end

    assert_equal 4, j
    override_current_time(s, s.next_time) # 95
    assert s.secondary?

    s.step
    assert_equal s.timeout_at, s.next_time # 100

    s.step
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
    5.times { |i| timeout += 1.0 * (2 ** i) }

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

  sub_test_case "ExponentialBackOff_ScenarioTests" do
    data("Simple timeout", {
      timeout: 100, max_steps: nil, max_interval: nil, use_sec: false, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 100, false),
      ],
    })
    data("Simple timeout with secondary", {
      timeout: 100, max_steps: nil, max_interval: nil, use_sec: true, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 80, true),
        RetryRecord.new(8, 81, true),
        RetryRecord.new(9, 83, true),
        RetryRecord.new(10, 87, true),
        RetryRecord.new(11, 95, true),
        RetryRecord.new(12, 100, true),
      ],
    })
    data("Simple timeout with custom wait and backoff_base", {
      timeout: 1000, max_steps: nil, max_interval: nil, use_sec: false, sec_thres: 0.8, wait: 2, backoff_base: 3,
      expected: [
        RetryRecord.new(1, 2, false),
        RetryRecord.new(2, 8, false),
        RetryRecord.new(3, 26, false),
        RetryRecord.new(4, 80, false),
        RetryRecord.new(5, 242, false),
        RetryRecord.new(6, 728, false),
        RetryRecord.new(7, 1000, false),
      ],
    })
    data("Simple timeout with custom wait and backoff_base and secondary", {
      timeout: 1000, max_steps: nil, max_interval: nil, use_sec: true, sec_thres: 0.8, wait: 2, backoff_base: 3,
      expected: [
        RetryRecord.new(1, 2, false),
        RetryRecord.new(2, 8, false),
        RetryRecord.new(3, 26, false),
        RetryRecord.new(4, 80, false),
        RetryRecord.new(5, 242, false),
        RetryRecord.new(6, 728, false),
        RetryRecord.new(7, 800, true),
        RetryRecord.new(8, 802, true),
        RetryRecord.new(9, 808, true),
        RetryRecord.new(10, 826, true),
        RetryRecord.new(11, 880, true),
        RetryRecord.new(12, 1000, true),
      ],
    })
    data("Default timeout", {
      timeout: 72*3600, max_steps: nil, max_interval: nil, use_sec: false, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
        RetryRecord.new(11, 2047, false),
        RetryRecord.new(12, 4095, false),
        RetryRecord.new(13, 8191, false),
        RetryRecord.new(14, 16383, false),
        RetryRecord.new(15, 32767, false),
        RetryRecord.new(16, 65535, false),
        RetryRecord.new(17, 131071, false),
        RetryRecord.new(18, 259200, false),
      ],
    })
    data("Default timeout with secondary", {
      timeout: 72*3600, max_steps: nil, max_interval: nil, use_sec: true, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
        RetryRecord.new(11, 2047, false),
        RetryRecord.new(12, 4095, false),
        RetryRecord.new(13, 8191, false),
        RetryRecord.new(14, 16383, false),
        RetryRecord.new(15, 32767, false),
        RetryRecord.new(16, 65535, false),
        RetryRecord.new(17, 131071, false),
        RetryRecord.new(18, 207360, true),
        RetryRecord.new(19, 207361, true),
        RetryRecord.new(20, 207363, true),
        RetryRecord.new(21, 207367, true),
        RetryRecord.new(22, 207375, true),
        RetryRecord.new(23, 207391, true),
        RetryRecord.new(24, 207423, true),
        RetryRecord.new(25, 207487, true),
        RetryRecord.new(26, 207615, true),
        RetryRecord.new(27, 207871, true),
        RetryRecord.new(28, 208383, true),
        RetryRecord.new(29, 209407, true),
        RetryRecord.new(30, 211455, true),
        RetryRecord.new(31, 215551, true),
        RetryRecord.new(32, 223743, true),
        RetryRecord.new(33, 240127, true),
        RetryRecord.new(34, 259200, true),
      ],
    })
    data("Default timeout with secondary and custom threshold", {
      timeout: 72*3600, max_steps: nil, max_interval: nil, use_sec: true, sec_thres: 0.5, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
        RetryRecord.new(11, 2047, false),
        RetryRecord.new(12, 4095, false),
        RetryRecord.new(13, 8191, false),
        RetryRecord.new(14, 16383, false),
        RetryRecord.new(15, 32767, false),
        RetryRecord.new(16, 65535, false),
        RetryRecord.new(17, 129600, true),
        RetryRecord.new(18, 129601, true),
        RetryRecord.new(19, 129603, true),
        RetryRecord.new(20, 129607, true),
        RetryRecord.new(21, 129615, true),
        RetryRecord.new(22, 129631, true),
        RetryRecord.new(23, 129663, true),
        RetryRecord.new(24, 129727, true),
        RetryRecord.new(25, 129855, true),
        RetryRecord.new(26, 130111, true),
        RetryRecord.new(27, 130623, true),
        RetryRecord.new(28, 131647, true),
        RetryRecord.new(29, 133695, true),
        RetryRecord.new(30, 137791, true),
        RetryRecord.new(31, 145983, true),
        RetryRecord.new(32, 162367, true),
        RetryRecord.new(33, 195135, true),
        RetryRecord.new(34, 259200, true),
      ],
    })
    data("Simple max_steps", {
      timeout: 72*3600, max_steps: 10, max_interval: nil, use_sec: false, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
      ],
    })
    data("Simple max_steps with secondary", {
      timeout: 72*3600, max_steps: 10, max_interval: nil, use_sec: true, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 818, true),
      ],
    })
    data("Simple interval", {
      timeout: 72*3600, max_steps: nil, max_interval: 3600, use_sec: false, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
        RetryRecord.new(11, 2047, false),
        RetryRecord.new(12, 4095, false),
        RetryRecord.new(13, 7695, false),
        RetryRecord.new(14, 11295, false),
        RetryRecord.new(15, 14895, false),
        RetryRecord.new(16, 18495, false),
        RetryRecord.new(17, 22095, false),
        RetryRecord.new(18, 25695, false),
        RetryRecord.new(19, 29295, false),
        RetryRecord.new(20, 32895, false),
        RetryRecord.new(21, 36495, false),
        RetryRecord.new(22, 40095, false),
        RetryRecord.new(23, 43695, false),
        RetryRecord.new(24, 47295, false),
        RetryRecord.new(25, 50895, false),
        RetryRecord.new(26, 54495, false),
        RetryRecord.new(27, 58095, false),
        RetryRecord.new(28, 61695, false),
        RetryRecord.new(29, 65295, false),
        RetryRecord.new(30, 68895, false),
        RetryRecord.new(31, 72495, false),
        RetryRecord.new(32, 76095, false),
        RetryRecord.new(33, 79695, false),
        RetryRecord.new(34, 83295, false),
        RetryRecord.new(35, 86895, false),
        RetryRecord.new(36, 90495, false),
        RetryRecord.new(37, 94095, false),
        RetryRecord.new(38, 97695, false),
        RetryRecord.new(39, 101295, false),
        RetryRecord.new(40, 104895, false),
        RetryRecord.new(41, 108495, false),
        RetryRecord.new(42, 112095, false),
        RetryRecord.new(43, 115695, false),
        RetryRecord.new(44, 119295, false),
        RetryRecord.new(45, 122895, false),
        RetryRecord.new(46, 126495, false),
        RetryRecord.new(47, 130095, false),
        RetryRecord.new(48, 133695, false),
        RetryRecord.new(49, 137295, false),
        RetryRecord.new(50, 140895, false),
        RetryRecord.new(51, 144495, false),
        RetryRecord.new(52, 148095, false),
        RetryRecord.new(53, 151695, false),
        RetryRecord.new(54, 155295, false),
        RetryRecord.new(55, 158895, false),
        RetryRecord.new(56, 162495, false),
        RetryRecord.new(57, 166095, false),
        RetryRecord.new(58, 169695, false),
        RetryRecord.new(59, 173295, false),
        RetryRecord.new(60, 176895, false),
        RetryRecord.new(61, 180495, false),
        RetryRecord.new(62, 184095, false),
        RetryRecord.new(63, 187695, false),
        RetryRecord.new(64, 191295, false),
        RetryRecord.new(65, 194895, false),
        RetryRecord.new(66, 198495, false),
        RetryRecord.new(67, 202095, false),
        RetryRecord.new(68, 205695, false),
        RetryRecord.new(69, 209295, false),
        RetryRecord.new(70, 212895, false),
        RetryRecord.new(71, 216495, false),
        RetryRecord.new(72, 220095, false),
        RetryRecord.new(73, 223695, false),
        RetryRecord.new(74, 227295, false),
        RetryRecord.new(75, 230895, false),
        RetryRecord.new(76, 234495, false),
        RetryRecord.new(77, 238095, false),
        RetryRecord.new(78, 241695, false),
        RetryRecord.new(79, 245295, false),
        RetryRecord.new(80, 248895, false),
        RetryRecord.new(81, 252495, false),
        RetryRecord.new(82, 256095, false),
        RetryRecord.new(83, 259200, false),
      ],
    })
    data("Simple interval with secondary", {
      timeout: 72*3600, max_steps: nil, max_interval: 3600, use_sec: true, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
        RetryRecord.new(11, 2047, false),
        RetryRecord.new(12, 4095, false),
        RetryRecord.new(13, 7695, false),
        RetryRecord.new(14, 11295, false),
        RetryRecord.new(15, 14895, false),
        RetryRecord.new(16, 18495, false),
        RetryRecord.new(17, 22095, false),
        RetryRecord.new(18, 25695, false),
        RetryRecord.new(19, 29295, false),
        RetryRecord.new(20, 32895, false),
        RetryRecord.new(21, 36495, false),
        RetryRecord.new(22, 40095, false),
        RetryRecord.new(23, 43695, false),
        RetryRecord.new(24, 47295, false),
        RetryRecord.new(25, 50895, false),
        RetryRecord.new(26, 54495, false),
        RetryRecord.new(27, 58095, false),
        RetryRecord.new(28, 61695, false),
        RetryRecord.new(29, 65295, false),
        RetryRecord.new(30, 68895, false),
        RetryRecord.new(31, 72495, false),
        RetryRecord.new(32, 76095, false),
        RetryRecord.new(33, 79695, false),
        RetryRecord.new(34, 83295, false),
        RetryRecord.new(35, 86895, false),
        RetryRecord.new(36, 90495, false),
        RetryRecord.new(37, 94095, false),
        RetryRecord.new(38, 97695, false),
        RetryRecord.new(39, 101295, false),
        RetryRecord.new(40, 104895, false),
        RetryRecord.new(41, 108495, false),
        RetryRecord.new(42, 112095, false),
        RetryRecord.new(43, 115695, false),
        RetryRecord.new(44, 119295, false),
        RetryRecord.new(45, 122895, false),
        RetryRecord.new(46, 126495, false),
        RetryRecord.new(47, 130095, false),
        RetryRecord.new(48, 133695, false),
        RetryRecord.new(49, 137295, false),
        RetryRecord.new(50, 140895, false),
        RetryRecord.new(51, 144495, false),
        RetryRecord.new(52, 148095, false),
        RetryRecord.new(53, 151695, false),
        RetryRecord.new(54, 155295, false),
        RetryRecord.new(55, 158895, false),
        RetryRecord.new(56, 162495, false),
        RetryRecord.new(57, 166095, false),
        RetryRecord.new(58, 169695, false),
        RetryRecord.new(59, 173295, false),
        RetryRecord.new(60, 176895, false),
        RetryRecord.new(61, 180495, false),
        RetryRecord.new(62, 184095, false),
        RetryRecord.new(63, 187695, false),
        RetryRecord.new(64, 191295, false),
        RetryRecord.new(65, 194895, false),
        RetryRecord.new(66, 198495, false),
        RetryRecord.new(67, 202095, false),
        RetryRecord.new(68, 205695, false),
        RetryRecord.new(69, 207360, true),
        RetryRecord.new(70, 207361, true),
        RetryRecord.new(71, 207363, true),
        RetryRecord.new(72, 207367, true),
        RetryRecord.new(73, 207375, true),
        RetryRecord.new(74, 207391, true),
        RetryRecord.new(75, 207423, true),
        RetryRecord.new(76, 207487, true),
        RetryRecord.new(77, 207615, true),
        RetryRecord.new(78, 207871, true),
        RetryRecord.new(79, 208383, true),
        RetryRecord.new(80, 209407, true),
        RetryRecord.new(81, 211455, true),
        RetryRecord.new(82, 215055, true),
        RetryRecord.new(83, 218655, true),
        RetryRecord.new(84, 222255, true),
        RetryRecord.new(85, 225855, true),
        RetryRecord.new(86, 229455, true),
        RetryRecord.new(87, 233055, true),
        RetryRecord.new(88, 236655, true),
        RetryRecord.new(89, 240255, true),
        RetryRecord.new(90, 243855, true),
        RetryRecord.new(91, 247455, true),
        RetryRecord.new(92, 251055, true),
        RetryRecord.new(93, 254655, true),
        RetryRecord.new(94, 258255, true),
        RetryRecord.new(95, 259200, true),
      ],
    })
    data("Max_steps and max_interval", {
      timeout: 72*3600, max_steps: 30, max_interval: 3600, use_sec: false, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
        RetryRecord.new(11, 2047, false),
        RetryRecord.new(12, 4095, false),
        RetryRecord.new(13, 7695, false),
        RetryRecord.new(14, 11295, false),
        RetryRecord.new(15, 14895, false),
        RetryRecord.new(16, 18495, false),
        RetryRecord.new(17, 22095, false),
        RetryRecord.new(18, 25695, false),
        RetryRecord.new(19, 29295, false),
        RetryRecord.new(20, 32895, false),
        RetryRecord.new(21, 36495, false),
        RetryRecord.new(22, 40095, false),
        RetryRecord.new(23, 43695, false),
        RetryRecord.new(24, 47295, false),
        RetryRecord.new(25, 50895, false),
        RetryRecord.new(26, 54495, false),
        RetryRecord.new(27, 58095, false),
        RetryRecord.new(28, 61695, false),
        RetryRecord.new(29, 65295, false),
        RetryRecord.new(30, 68895, false),
      ],
    })
    data("Max_steps and max_interval with secondary", {
      timeout: 72*3600, max_steps: 30, max_interval: 3600, use_sec: true, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
        RetryRecord.new(11, 2047, false),
        RetryRecord.new(12, 4095, false),
        RetryRecord.new(13, 7695, false),
        RetryRecord.new(14, 11295, false),
        RetryRecord.new(15, 14895, false),
        RetryRecord.new(16, 18495, false),
        RetryRecord.new(17, 22095, false),
        RetryRecord.new(18, 25695, false),
        RetryRecord.new(19, 29295, false),
        RetryRecord.new(20, 32895, false),
        RetryRecord.new(21, 36495, false),
        RetryRecord.new(22, 40095, false),
        RetryRecord.new(23, 43695, false),
        RetryRecord.new(24, 47295, false),
        RetryRecord.new(25, 50895, false),
        RetryRecord.new(26, 54495, false),
        RetryRecord.new(27, 55116, true),
        RetryRecord.new(28, 55117, true),
        RetryRecord.new(29, 55119, true),
        RetryRecord.new(30, 55123, true),
      ],
    })
    data("Max_steps and max_interval with timeout", {
      timeout: 10000, max_steps: 30, max_interval: 1000, use_sec: false, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
        RetryRecord.new(11, 2023, false),
        RetryRecord.new(12, 3023, false),
        RetryRecord.new(13, 4023, false),
        RetryRecord.new(14, 5023, false),
        RetryRecord.new(15, 6023, false),
        RetryRecord.new(16, 7023, false),
        RetryRecord.new(17, 8023, false),
        RetryRecord.new(18, 9023, false),
        RetryRecord.new(19, 10000, false),
      ],
    })
    data("Max_steps and max_interval with timeout and secondary", {
      timeout: 10000, max_steps: 30, max_interval: 1000, use_sec: true, sec_thres: 0.8, wait: 1, backoff_base: 2,
      expected: [
        RetryRecord.new(1, 1, false),
        RetryRecord.new(2, 3, false),
        RetryRecord.new(3, 7, false),
        RetryRecord.new(4, 15, false),
        RetryRecord.new(5, 31, false),
        RetryRecord.new(6, 63, false),
        RetryRecord.new(7, 127, false),
        RetryRecord.new(8, 255, false),
        RetryRecord.new(9, 511, false),
        RetryRecord.new(10, 1023, false),
        RetryRecord.new(11, 2023, false),
        RetryRecord.new(12, 3023, false),
        RetryRecord.new(13, 4023, false),
        RetryRecord.new(14, 5023, false),
        RetryRecord.new(15, 6023, false),
        RetryRecord.new(16, 7023, false),
        RetryRecord.new(17, 8000, true),
        RetryRecord.new(18, 8001, true),
        RetryRecord.new(19, 8003, true),
        RetryRecord.new(20, 8007, true),
        RetryRecord.new(21, 8015, true),
        RetryRecord.new(22, 8031, true),
        RetryRecord.new(23, 8063, true),
        RetryRecord.new(24, 8127, true),
        RetryRecord.new(25, 8255, true),
        RetryRecord.new(26, 8511, true),
        RetryRecord.new(27, 9023, true),
        RetryRecord.new(28, 10000, true),
      ],
    })
    test "exponential backoff with senario" do |data|
      print_for_debug = false # change this value true if need to see msg always.
      trying_count = 1000 # just for avoiding infinite loop

      retry_records = []
      msg = ""

      s = @d.retry_state_create(
        :t15, :exponential_backoff, data[:wait], data[:timeout],
        max_steps: data[:max_steps], max_interval: data[:max_interval],
        secondary: data[:use_sec], secondary_threshold: data[:sec_thres],
        backoff_base: data[:backoff_base], randomize: false
      )
      override_current_time(s, s.start)

      retry_count = 0
      trying_count.times do
        next_elapsed = (s.next_time - s.start).to_i

        msg << "step: #{s.steps}, next: #{next_elapsed}s (#{next_elapsed / 3600}h)\n"

        # Wait until next time to trigger the next retry
        override_current_time(s, s.next_time)

        # Retry will be triggered at this point.
        retry_count += 1
        rec = RetryRecord.new(retry_count, next_elapsed, s.secondary?)
        retry_records.append(rec)
        msg << "[#{next_elapsed}s elapsed point] #{retry_count}th-Retry(#{s.secondary? ? "SEC" : "PRI"}) is triggered.\n"

        # Update retry statement
        s.step
        if s.limit?
          msg << "--- Reach limit. ---\n"
          break
        end
      end

      assert_equal(data[:expected], retry_records, msg)

      print(msg) if print_for_debug
    end
  end
end
