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

require 'fluent/plugin/input'

module Fluent::Plugin
  class TailInput < Fluent::Plugin::Input
    module GroupWatchParams
      include Fluent::Configurable

      DEFAULT_KEY = /.*/
      DEFAULT_LIMIT = -1
      REGEXP_JOIN = "_"

      config_section :group, param_name: :group, required: false, multi: false do
        desc 'Regex for extracting group\'s metadata'
        config_param :pattern,
                     :regexp,
                     default: /^\/var\/log\/containers\/(?<podname>[a-z0-9]([-a-z0-9]*[a-z0-9])?(\/[a-z0-9]([-a-z0-9]*[a-z0-9])?)*)_(?<namespace>[^_]+)_(?<container>.+)-(?<docker_id>[a-z0-9]{64})\.log$/

        desc 'Period of time in which the group_line_limit is applied'
        config_param :rate_period, :time, default: 5

        config_section :rule, param_name: :rule, required: true, multi: true do
          desc 'Key-value pairs for grouping'
          config_param :match, :hash, value_type: :regexp, default: {namespace: [DEFAULT_KEY], podname: [DEFAULT_KEY]}
          desc 'Maximum number of log lines allowed per group over a period of rate_period'
          config_param :limit, :integer, default: DEFAULT_LIMIT
        end
      end
    end

    module GroupWatch
      def self.included(mod)
        mod.include GroupWatchParams
      end

      attr_reader :group_watchers, :default_group_key

      def initialize
        super
        @group_watchers = {}
        @group_keys = nil
        @default_group_key = nil
      end

      def configure(conf)
        super

        unless @group.nil?
          ## Ensuring correct time period syntax
          @group.rule.each { |rule|
            raise "Metadata Group Limit >= DEFAULT_LIMIT" unless rule.limit >= GroupWatchParams::DEFAULT_LIMIT
          }

          @group_keys = Regexp.compile(@group.pattern).named_captures.keys
          @default_group_key = ([GroupWatchParams::DEFAULT_KEY] * @group_keys.length).join(GroupWatchParams::REGEXP_JOIN)

          ## Ensures that "specific" rules (with larger number of `rule.match` keys)
          ## have a higher priority against "generic" rules (with less number of `rule.match` keys).
          ## This will be helpful when a file satisfies more than one rule.
          @group.rule.sort_by!{ |rule| -rule.match.length() }
          construct_groupwatchers
          @group_watchers[@default_group_key] ||= GroupWatcher.new(@group.rate_period, GroupWatchParams::DEFAULT_LIMIT)
        end
      end

      def construct_group_key(named_captures)
        match_rule = []
        @group_keys.each { |key|
          match_rule.append(named_captures.fetch(key, GroupWatchParams::DEFAULT_KEY))
        }
        match_rule = match_rule.join(GroupWatchParams::REGEXP_JOIN)

        match_rule
      end

      def construct_groupwatchers
        @group.rule.each { |rule|
          match_rule = construct_group_key(rule.match)
          @group_watchers[match_rule] ||= GroupWatcher.new(@group.rate_period, rule.limit)
        }
      end

      def find_group(metadata)
        metadata_key = construct_group_key(metadata)
        gw_key = @group_watchers.keys.find{ |regexp| metadata_key.match?(regexp) && regexp != @default_group_key}
        gw_key ||= @default_group_key

        @group_watchers[gw_key]
      end

      def find_group_from_metadata(path)
        begin
          metadata = @group.pattern.match(path).named_captures
          group_watcher = find_group(metadata)
        rescue => e
          log.warn "Cannot find group from metadata, Adding file in the default group"
          group_watcher = @group_watchers[@default_group_key]
        end

        group_watcher
      end
    end

    class GroupWatcher
      attr_accessor :current_paths, :limit, :number_lines_read, :start_reading_time, :rate_period

      FileCounter = Struct.new(
        :number_lines_read,
        :start_reading_time,
      )

      def initialize(rate_period = 60, limit = -1)
        @current_paths = {}
        @rate_period = rate_period
        @limit = limit
      end

      def add(path)
        @current_paths[path] = FileCounter.new(0, nil)
      end

      def include?(path)
        @current_paths.key?(path)
      end

      def size
        @current_paths.size
      end

      def delete(path)
        @current_paths.delete(path)
      end

      def update_reading_time(path)
        @current_paths[path].start_reading_time ||= Fluent::Clock.now
      end

      def update_lines_read(path, value)
        @current_paths[path].number_lines_read += value
      end

      def reset_counter(path)
        @current_paths[path].start_reading_time = nil
        @current_paths[path].number_lines_read = 0
      end

      def time_spent_reading(path)
        Fluent::Clock.now - @current_paths[path].start_reading_time
      end

      def limit_time_period_reached?(path)
        time_spent_reading(path) < @rate_period
      end

      def limit_lines_reached?(path)
        return true unless include?(path)
        return true if @limit == 0

        return false if @limit < 0
        return false if @current_paths[path].number_lines_read < @limit / size

        # update_reading_time(path)
        if limit_time_period_reached?(path) # Exceeds limit
          true
        else # Does not exceed limit
          reset_counter(path)
          false
        end
      end

      def to_s
        super + " current_paths: #{@current_paths} rate_period: #{@rate_period} limit: #{@limit}"
      end
    end
  end
end
