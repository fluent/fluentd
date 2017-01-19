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
  module Clock
    CLOCK_ID = Process::CLOCK_MONOTONIC_RAW rescue Process::CLOCK_MONOTONIC

    @@block_level = 0
    @@frozen_clock = nil

    def self.now
      @@frozen_clock || now_raw
    end

    def self.freeze(dst = nil, &block)
      return freeze_block(dst, &block) if block_given?

      dst = dst_clock_from_time(dst) if dst.is_a?(Time)
      @@frozen_clock = dst || now_raw
    end

    def self.return
      raise "invalid return while running code in blocks" if @@block_level > 0
      @@frozen_clock = nil
    end

    # internal use

    def self.now_raw
      Process.clock_gettime(CLOCK_ID)
    end

    def self.dst_clock_from_time(time)
      diff_sec = Time.now - time
      now_raw - diff_sec
    end

    def self.freeze_block(dst)
      dst = dst_clock_from_time(dst) if dst.is_a?(Time)
      pre_frozen_clock = @@frozen_clock
      @@frozen_clock = dst || now_raw
      @@block_level += 1
      yield
    ensure
      @@block_level -= 1
      @@frozen_clock = pre_frozen_clock
    end
  end
end
