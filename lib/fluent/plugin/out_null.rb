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

require 'fluent/plugin/output'

module Fluent::Plugin
  class NullOutput < Output
    # This plugin is for tests of non-buffered/buffered plugins
    Fluent::Plugin.register_output('null', self)

    desc "The parameter for testing to simulate output plugin which never succeed to flush."
    config_param :never_flush, :bool, default: false

    config_section :buffer do
      config_set_default :chunk_keys, ['tag']
      config_set_default :flush_at_shutdown, true
      config_set_default :chunk_limit_size, 10 * 1024
    end

    def prefer_buffered_processing
      false
    end

    def prefer_delayed_commit
      @delayed
    end

    attr_accessor :feed_proc, :delayed

    def initialize
      super
      @delayed = false
      @feed_proc = nil
    end

    def multi_workers_ready?
      true
    end

    def process(tag, es)
      raise "failed to flush" if @never_flush
      # Do nothing
    end

    def write(chunk)
      raise "failed to flush" if @never_flush
      if @feed_proc
        @feed_proc.call(chunk)
      end
    end

    def try_write(chunk)
      raise "failed to flush" if @never_flush
      if @feed_proc
        @feed_proc.call(chunk)
      end
      # not to commit chunks for testing
      # commit_write(chunk.unique_id)
    end
  end
end
