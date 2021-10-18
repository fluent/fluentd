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

require 'fluent/plugin/in_tail'

module Fluent::Plugin
  class ThrottleInput < Fluent::Plugin::TailInput
    Fluent::Plugin.register_input('tail_with_throttle', self)
    
    DEFAULT_NAMESPACE = DEFAULT_APPNAME = /./
    DEFAULT_LIMIT = -1

    attr_reader :group_watchers

    def initialize
      super
      # Map rules with GroupWatcher objects
      @group_watchers = {}
      @sorted_path = nil
    end

    config_section :group, param_name: :group, required: true, multi: false do
      desc 'Regex for extracting group\'s metadata'
      config_param :pattern, 
                   :regexp, 
                   default: /var\/log\/containers\/(?<appname>[a-z0-9]([-a-z0-9]*[a-z0-9])?(\/[a-z0-9]([-a-z0-9]*[a-z0-9])?)*)_(?<namespace>[^_]+)_(?<container>.+)-(?<docker_id>[a-z0-9]{64})\.log$/
      desc 'Period of time in which the group_line_limit is applied'
      config_param :rate_period, :time, default: 5

      config_section :rule, multi: true, required: true do
        desc 'Namespace key'
        config_param :namespace, :array, value_type: :string, default: [DEFAULT_NAMESPACE]
        desc 'App name key'
        config_param :appname, :array, value_type: :string, default: [DEFAULT_APPNAME]
        desc 'Maximum number of log lines allowed per group over a period of rate_period'
        config_param :limit, :integer, default: DEFAULT_LIMIT
      end
    end

    def configure(conf)
      super
      ## Ensuring correct time period syntax
      @group.rule.each { |rule| 
        raise "Metadata Group Limit >= DEFAULT_LIMIT" unless rule.limit >= DEFAULT_LIMIT
      }

      construct_groupwatchers
    end

    def construct_groupwatchers
      @group.rule.each { |rule|
        num_groups = rule.namespace.size * rule.appname.size
        
        rule.namespace.each { |namespace| 
          namespace = /#{Regexp.quote(namespace)}/ unless namespace.eql?(DEFAULT_NAMESPACE)
          @group_watchers[namespace] ||= {}

          rule.appname.each { |appname|
            appname = /#{Regexp.quote(appname)}/ unless appname.eql?(DEFAULT_APPNAME)
            @group_watchers[namespace][appname] = GroupWatcher.new(@group.rate_period, rule.limit/num_groups)
          }

          @group_watchers[namespace][DEFAULT_APPNAME] ||= GroupWatcher.new(@group.rate_period)
        }
      }

      if @group_watchers.dig(DEFAULT_NAMESPACE, DEFAULT_APPNAME).nil?
        @group_watchers[DEFAULT_NAMESPACE] ||= {}
        @group_watchers[DEFAULT_NAMESPACE][DEFAULT_APPNAME] = GroupWatcher.new(@group.rate_period)
      end
    end

    def find_group_from_metadata(path)
      def find_group(namespace, appname)
        namespace_key = @group_watchers.keys.find { |regexp| namespace.match?(regexp) && regexp != DEFAULT_NAMESPACE }
        namespace_key ||= DEFAULT_NAMESPACE

        appname_key = @group_watchers[namespace_key].keys.find { |regexp| appname.match?(regexp) && regexp != DEFAULT_APPNAME }
        appname_key ||= DEFAULT_APPNAME

        @group_watchers[namespace_key][appname_key]
      end
  
      begin
        metadata = @group.pattern.match(path)
        group_watcher = find_group(metadata['namespace'], metadata['appname'])
      rescue => e
        $log.warn "Cannot find group from metadata, Adding file in the default group"
        group_watcher = @group_watchers[DEFAULT_NAMESPACE][DEFAULT_APPNAME] 
      end

      group_watcher
    end

    def stop_watchers(targets_info, immediate: false, unwatched: false, remove_watcher: true)
      targets_info.each_value { |target_info|
        group_watcher = find_group_from_metadata(target_info.path)
        group_watcher.delete(target_info.path)
      }
      super
    end

    def setup_watcher(target_info, pe)
      tw = super
      group_watcher = find_group_from_metadata(target_info.path)
      group_watcher.add(tw.path) unless group_watcher.include?(tw.path)
      tw.group_watcher = group_watcher

      tw
      rescue => e 
        raise e
    end

    def detach_watcher_after_rotate_wait(tw, ino)
      # Call event_loop_attach/event_loop_detach is high-cost for short-live object.
      # If this has a problem with large number of files, use @_event_loop directly instead of timer_execute.
      if @open_on_every_update
        # Detach now because it's already closed, waiting it doesn't make sense.
        detach_watcher(tw, ino)
      elsif !tw.group_watcher.nil? && tw.group_watcher.limit <= 0
        # throttling isn't enabled, just wait @rotate_wait
        timer_execute(:in_tail_close_watcher, @rotate_wait, repeat: false) do
          detach_watcher(tw, ino)
        end
      else
        # When the throttling feature is enabled, it might not reach EOF yet.
        # Should ensure to read all contents before closing it, with keeping throttling.
        start_time_to_wait = Fluent::Clock.now
        timer = timer_execute(:in_tail_close_watcher, 1, repeat: true) do
          elapsed = Fluent::Clock.now - start_time_to_wait
          if tw.eof? && elapsed >= @rotate_wait
            timer.detach
            detach_watcher(tw, ino)
          end
        end
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

    class Fluent::Plugin::TailInput::TailWatcher
      attr_accessor :group_watcher

      def group_watcher=(group_watcher)
        @group_watcher = group_watcher
      end


      class Fluent::Plugin::TailInput::TailWatcher::IOHandler
        alias_method :orig_handle_notify, :handle_notify

        def group_watcher
          @watcher.group_watcher
        end

        def handle_notify
          if group_watcher.nil?
            orig_handle_notify
          else
            rate_limit_handle_notify
          end
        end
      
        def rate_limit_handle_notify
          return if group_watcher.limit_lines_reached?(@path)

          with_io do |io|
            begin
              read_more = false

              if !io.nil? && @lines.empty?
                begin
                  while true
                    group_watcher.update_reading_time(@path)
                    data = io.readpartial(BYTES_TO_READ, @iobuf)
                    @eof = false
                    @fifo << data
                    group_watcher.update_lines_read(@path, -@lines.size)
                    @fifo.read_lines(@lines)
                    group_watcher.update_lines_read(@path, @lines.size)

                    if group_watcher.limit_lines_reached?(@path) || should_shutdown_now?
                      # Just get out from tailing loop.
                      @log.info "Read limit exceeded #{@path}" if !should_shutdown_now? 
                      read_more = false
                      break
                    elsif @lines.size >= @read_lines_limit
                      # not to use too much memory in case the file is very large
                      read_more = true
                      break
                    end
                  end
                rescue EOFError
                  @eof = true
                end
              end
              @log.debug "Lines read: #{@path} #{group_watcher.current_paths[@path].number_lines_read}"

              unless @lines.empty?
                if @receive_lines.call(@lines, @watcher)
                  @watcher.pe.update_pos(io.pos - @fifo.bytesize)
                  @lines.clear
                else
                  read_more = false
                end
              end
            end while read_more
          end
        end
      end      
    end
  end
end