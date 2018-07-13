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

require 'delegate'

require 'fluent/config/error'
require 'fluent/agent'
require 'fluent/label'
require 'fluent/plugin'
require 'fluent/system_config'
require 'fluent/time'

module Fluent
  #
  # Fluentd forms a tree structure to manage plugins:
  #
  #                      RootAgent
  #                          |
  #             +------------+-------------+-------------+
  #             |            |             |             |
  #          <label>      <source>      <filter>      <match>
  #             |
  #        +----+----+
  #        |         |
  #     <filter>   <match>
  #
  # Relation:
  # * RootAgent has many <label>, <source>, <filter> and <match>
  # * <label>   has many <match> and <filter>
  #
  # Next step: `fluentd/agent.rb`
  # Next step: 'fluentd/label.rb'
  #
  class RootAgent < Agent
    ERROR_LABEL = "@ERROR".freeze # @ERROR is built-in error label

    def initialize(log:, system_config: SystemConfig.new)
      super(log: log)

      @labels = {}
      @inputs = []
      @suppress_emit_error_log_interval = 0
      @next_emit_error_log_time = nil
      @without_source = false

      suppress_interval(system_config.emit_error_log_interval) unless system_config.emit_error_log_interval.nil?
      @without_source = system_config.without_source unless system_config.without_source.nil?
    end

    attr_reader :inputs
    attr_reader :labels

    def configure(conf)
      # initialize <worker> elements
      conf.elements(name: 'worker').each do |e|
        target_worker_id_str = e.arg
        if target_worker_id_str.empty?
          raise ConfigError, "Missing worker id on <worker> directive"
        end

        target_worker_id = target_worker_id_str.to_i
        if target_worker_id < 0 || target_worker_id > (Fluent::Engine.system_config.workers - 1)
          raise ConfigError, "worker id #{target_worker_id} specified by <worker> directive is not allowed. Available worker id is between 0 and #{(Fluent::Engine.system_config.workers - 1)}"
        end

        ## On dry_run mode, all worker sections have to be configured on supervisor (recognized as worker_id = 0).
        target_worker_id = 0 if Fluent::Engine.dry_run_mode

        e.elements.each do |elem|
          unless ['source', 'match', 'filter', 'label'].include?(elem.name)
            raise ConfigError, "<worker> section cannot have <#{elem.name}> directive"
          end
          elem.set_target_worker_id(target_worker_id)
        end
        conf += e
      end
      conf.elements.delete_if{|e| e.name == 'worker'}

      error_label_config = nil

      # initialize <label> elements before configuring all plugins to avoid 'label not found' in input, filter and output.
      label_configs = {}
      conf.elements(name: 'label').each { |e|
        next if e.for_another_worker?
        name = e.arg
        raise ConfigError, "Missing symbol argument on <label> directive" if name.empty?

        if name == ERROR_LABEL
          error_label_config = e
        else
          add_label(name)
          label_configs[name] = e
        end
      }
      # Call 'configure' here to avoid 'label not found'
      label_configs.each { |name, e| @labels[name].configure(e) }
      setup_error_label(error_label_config) if error_label_config

      super

      # initialize <source> elements
      if @without_source
        log.info :worker0, "'--without-source' is applied. Ignore <source> sections"
      else
        conf.elements(name: 'source').each { |e|
          next if e.for_another_worker?
          type = e['@type']
          raise ConfigError, "Missing '@type' parameter on <source> directive" unless type
          add_source(type, e)
        }
      end
    end

    def setup_error_label(e)
      error_label = add_label(ERROR_LABEL)
      error_label.configure(e)
      error_label.root_agent = RootAgentProxyWithoutErrorCollector.new(self)
      @error_collector = error_label.event_router
    end

    def lifecycle(desc: false, kind_callback: nil)
      kind_or_label_list = if desc
                    [:output, :filter, @labels.values.reverse, :output_with_router, :input].flatten
                  else
                    [:input, :output_with_router, @labels.values, :filter, :output].flatten
                  end
      kind_or_label_list.each do |kind|
        if kind.respond_to?(:lifecycle)
          label = kind
          label.lifecycle(desc: desc) do |plugin, display_kind|
            yield plugin, display_kind
          end
        else
          list = if desc
                   lifecycle_control_list[kind].reverse
                 else
                   lifecycle_control_list[kind]
                 end
          display_kind = (kind == :output_with_router ? :output : kind)
          list.each do |instance|
            yield instance, display_kind
          end
        end
        if kind_callback
          kind_callback.call
        end
      end
    end

    def start
      lifecycle(desc: true) do |i| # instance
        i.start unless i.started?
        # Input#start sometimes emits lots of evetns with in_tail/`read_from_head true` case
        # and it causes deadlock for small buffer/queue output. To avoid such problem,
        # buffer related output threads should be run before `Input#start`.
        # This is why after_start should be called immediately after start call.
        # This depends on `desc: true` because calling plugin order of `desc: true` is
        # Output, Filter, Label, Output with Router, then Input.
        i.after_start unless i.after_started?
      end
    end

    def flush!
      log.info "flushing all buffer forcedly"
      flushing_threads = []
      lifecycle(desc: true) do |instance|
        if instance.respond_to?(:force_flush)
          t = Thread.new do
            Thread.current.abort_on_exception = true
            begin
              instance.force_flush
            rescue => e
              log.warn "unexpected error while flushing buffer", plugin: instance.class, plugin_id: instance.plugin_id, error: e
              log.warn_backtrace
            end
          end
          flushing_threads << t
        end
      end
      flushing_threads.each{|t| t.join }
    end

    def shutdown # Fluentd's shutdown sequence is stop, before_shutdown, shutdown, after_shutdown, close, terminate for plugins
      # These method callers does `rescue Exception` to call methods of shutdown sequence as far as possible
      # if plugin methods does something like infinite recursive call, `exit`, unregistering signal handlers or others.
      # Plugins should be separated and be in sandbox to protect data in each plugins/buffers.

      lifecycle_safe_sequence = ->(method, checker) {
        lifecycle do |instance, kind|
          begin
            log.debug "calling #{method} on #{kind} plugin", type: Plugin.lookup_type_from_class(instance.class), plugin_id: instance.plugin_id
            instance.send(method) unless instance.send(checker)
          rescue Exception => e
            log.warn "unexpected error while calling #{method} on #{kind} plugin", plugin: instance.class, plugin_id: instance.plugin_id, error: e
            log.warn_backtrace
          end
        end
      }

      lifecycle_unsafe_sequence = ->(method, checker) {
        operation = case method
                    when :shutdown then "shutting down"
                    when :close    then "closing"
                    else
                      raise "BUG: unknown method name '#{method}'"
                    end
        operation_threads = []
        callback = ->(){
          operation_threads.each{|t| t.join }
          operation_threads.clear
        }
        lifecycle(kind_callback: callback) do |instance, kind|
          t = Thread.new do
            Thread.current.abort_on_exception = true
            begin
              if method == :shutdown
                # To avoid Input#shutdown and Output#before_shutdown mismatch problem, combine before_shutdown and shutdown call in one sequence.
                # The problem is in_tail flushes buffered multiline in shutdown but output's flush_at_shutdown is invoked in before_shutdown
                operation = "preparing shutdown" # for logging
                log.debug "#{operation} #{kind} plugin", type: Plugin.lookup_type_from_class(instance.class), plugin_id: instance.plugin_id
                begin
                  instance.send(:before_shutdown) unless instance.send(:before_shutdown?)
                rescue Exception => e
                  log.warn "unexpected error while #{operation} on #{kind} plugin", plugin: instance.class, plugin_id: instance.plugin_id, error: e
                  log.warn_backtrace
                end
                operation = "shutting down"
                log.info "#{operation} #{kind} plugin", type: Plugin.lookup_type_from_class(instance.class), plugin_id: instance.plugin_id
                instance.send(:shutdown) unless instance.send(:shutdown?)
              else
                log.debug "#{operation} #{kind} plugin", type: Plugin.lookup_type_from_class(instance.class), plugin_id: instance.plugin_id
                instance.send(method) unless instance.send(checker)
              end
            rescue Exception => e
              log.warn "unexpected error while #{operation} on #{kind} plugin", plugin: instance.class, plugin_id: instance.plugin_id, error: e
              log.warn_backtrace
            end
          end
          operation_threads << t
        end
      }

      lifecycle_safe_sequence.call(:stop, :stopped?)

      # before_shutdown does force_flush for output plugins: it should block, so it's unsafe operation
      lifecycle_unsafe_sequence.call(:shutdown, :shutdown?)

      lifecycle_safe_sequence.call(:after_shutdown, :after_shutdown?)

      lifecycle_unsafe_sequence.call(:close, :closed?)

      lifecycle_safe_sequence.call(:terminate, :terminated?)
    end

    def suppress_interval(interval_time)
      @suppress_emit_error_log_interval = interval_time
      @next_emit_error_log_time = Time.now.to_i
    end

    def add_source(type, conf)
      log_type = conf.for_this_worker? ? :default : :worker0
      log.info log_type, "adding source", type: type

      input = Plugin.new_input(type)
      # <source> emits events to the top-level event router (RootAgent#event_router).
      # Input#configure overwrites event_router to a label's event_router if it has `@label` parameter.
      # See also 'fluentd/plugin/input.rb'
      input.context_router = @event_router
      input.configure(conf)
      @inputs << input

      input
    end

    def add_label(name)
      label = Label.new(name, log: log)
      raise ConfigError, "Section <label #{name}> appears twice" if @labels[name]
      label.root_agent = self
      @labels[name] = label
    end

    def find_label(label_name)
      if label = @labels[label_name]
        label
      else
        raise ArgumentError, "#{label_name} label not found"
      end
    end

    def emit_error_event(tag, time, record, error)
      error_info = {error: error, location: (error.backtrace ? error.backtrace.first : nil), tag: tag, time: time}
      if @error_collector
        # A record is not included in the logs because <@ERROR> handles it. This warn is for the notification
        log.warn "send an error event to @ERROR:", error_info
        @error_collector.emit(tag, time, record)
      else
        error_info[:record] = record
        log.warn "dump an error event:", error_info
      end
    end

    def handle_emits_error(tag, es, error)
      error_info = {error: error, location: (error.backtrace ? error.backtrace.first : nil), tag: tag}
      if @error_collector
        log.warn "send an error event stream to @ERROR:", error_info
        @error_collector.emit_stream(tag, es)
      else
        now = Time.now.to_i
        if @suppress_emit_error_log_interval.zero? || now > @next_emit_error_log_time
          log.warn "emit transaction failed:", error_info
          log.warn_backtrace
          @next_emit_error_log_time = now + @suppress_emit_error_log_interval
        end
        raise error
      end
    end

    # <label @ERROR> element use RootAgent wrapped by # this RootAgentProxyWithoutErrorCollector.
    # So that those elements don't send cause infinite loop.
    class RootAgentProxyWithoutErrorCollector < SimpleDelegator
      def initialize(root_agent)
        super

        @suppress_emit_error_log_interval = 0
        @next_emit_error_log_time = nil

        interval_time = root_agent.instance_variable_get(:@suppress_emit_error_log_interval)
        suppress_interval(interval_time) unless interval_time.zero?
      end

      def emit_error_event(tag, time, record, error)
        error_info = {error: error, location: (error.backtrace ? error.backtrace.first : nil), tag: tag, time: time, record: record}
        log.warn "dump an error event in @ERROR:", error_info
      end

      def handle_emits_error(tag, es, e)
        now = EventTime.now.to_i
        if @suppress_emit_error_log_interval.zero? || now > @next_emit_error_log_time
          log.warn "emit transaction failed in @ERROR:", error: e, tag: tag
          log.warn_backtrace
          @next_emit_error_log_time = now + @suppress_emit_error_log_interval
        end
        raise e
      end
    end
  end
end
