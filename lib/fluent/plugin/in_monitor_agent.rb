# frozen_string_literal: true

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

require 'json'
require 'webrick'
require 'cgi'

require 'fluent/config/types'
require 'fluent/plugin/input'
require 'fluent/plugin/output'
require 'fluent/plugin/multi_output'
require 'fluent/plugin/filter'

module Fluent::Plugin
  class MonitorAgentInput < Input
    Fluent::Plugin.register_input('monitor_agent', self)

    helpers :timer, :thread, :http_server

    desc 'The address to bind to.'
    config_param :bind, :string, default: '0.0.0.0'
    desc 'The port to listen to.'
    config_param :port, :integer, default: 24220
    desc 'The tag with which internal metrics are emitted.'
    config_param :tag, :string, default: nil
    desc 'Determine the rate to emit internal metrics as events.'
    config_param :emit_interval, :time, default: 60
    desc 'Determine whether to include the config information.'
    config_param :include_config, :bool, default: true
    desc 'Determine whether to include the retry information.'
    config_param :include_retry, :bool, default: true

    class APIHandler
      def initialize(agent)
        @agent = agent
      end

      def plugins_ltsv(req)
        list = build_object(build_option(req))

        render_ltsv(list)
      end

      def plugins_json(req)
        opts = build_option(req)
        obj = build_object(opts)

        render_json({ 'plugins' => obj }, pretty_json: opts[:pretty_json])
      end

      def config_ltsv(_req)
        obj = {
          'pid' => Process.pid,
          'ppid' => Process.ppid,
          'version' => Fluent::VERSION,
        }.merge(@agent.fluentd_opts)

        render_ltsv([obj])
      end

      def config_json(req)
        obj = {
          'pid' => Process.pid,
          'ppid' => Process.ppid,
          'version' => Fluent::VERSION,
        }.merge(@agent.fluentd_opts)
        opts = build_option(req)

        render_json(obj, pretty_json: opts[:pretty_json])
      end

      private

      def render_error_json(code:, msg:, pretty_json: nil, **additional_params)
        resp = additional_params.merge('message' => msg)
        render_json(resp, code: code, pretty_json: pretty_json)
      end

      def render_json(obj, code: 200, pretty_json: nil)
        body =
          if pretty_json
            JSON.pretty_generate(obj)
          else
            obj.to_json
          end

        [code, { 'Content-Type' => 'application/json' }, body]
      end

      def render_ltsv(obj, code: 200)
        normalized = JSON.parse(obj.to_json)
        text = +''
        normalized.each do |hash|
          row = []
          hash.each do |k, v|
            if v.is_a?(Array)
              row << "#{k}:#{v.join(',')}"
            elsif v.is_a?(Hash)
              next
            else
              row << "#{k}:#{v}"
            end
          end

          text << row.join("\t") << "\n"
        end

        [code, { 'Content-Type' => 'text/plain' }, text]
      end

      def build_object(opts)
        qs = opts[:query]
        if tag = qs['tag'].first
          # ?tag= to search an output plugin by match pattern
          if obj = @agent.plugin_info_by_tag(tag, opts)
            list = [obj]
          else
            list = []
          end
        elsif plugin_id = (qs['@id'].first || qs['id'].first)
          # ?@id= to search a plugin by 'id <plugin_id>' config param
          if obj = @agent.plugin_info_by_id(plugin_id, opts)
            list = [obj]
          else
            list = []
          end
        elsif plugin_type = (qs['@type'].first || qs['type'].first)
          # ?@type= to search plugins by 'type <type>' config param
          list = @agent.plugins_info_by_type(plugin_type, opts)
        else
          # otherwise show all plugins
          list = @agent.plugins_info_all(opts)
        end

        list
      end

      def build_option(req)
        qs = Hash.new { |_, _| [] }
        # parse ?=query string
        if req.query_string
          qs.merge!(CGI.parse(req.query_string))
        end

        # if ?debug=1 is set, set :with_debug_info for get_monitor_info
        # and :pretty_json for render_json_error
        opts = { query: qs }
        if qs['debug'].first
          opts[:with_debug_info] = true
          opts[:pretty_json] = true
        end

        if ivars = qs['with_ivars'].first
          opts[:ivars] = ivars.split(',')
        end

        if with_config = qs['with_config'].first
          opts[:with_config] = Fluent::Config.bool_value(with_config)
        else
          opts[:with_config] = @agent.include_config
        end

        if with_retry = qs['with_retry'].first
          opts[:with_retry] = Fluent::Config.bool_value(with_retry)
        else
          opts[:with_retry] = @agent.include_retry
        end

        opts
      end
    end

    def initialize
      super

      @first_warn = false
    end

    def configure(conf)
      super
      @port += fluentd_worker_id
    end

    def multi_workers_ready?
      true
    end

    class NotFoundJson
      BODY = { 'message' => 'Not found' }.to_json
      def self.call(_req)
        [404, { 'Content-Type' => 'application/json' }, BODY]
      end
    end

    def start
      super

      log.debug "listening monitoring http server on http://#{@bind}:#{@port}/api/plugins for worker#{fluentd_worker_id}"
      api_handler = APIHandler.new(self)
      http_server_create_http_server(:in_monitor_http_server_helper, addr: @bind, port: @port, logger: log, default_app: NotFoundJson) do |serv|
        serv.get('/api/plugins') { |req| api_handler.plugins_ltsv(req) }
        serv.get('/api/plugins.json') { |req| api_handler.plugins_json(req) }
        serv.get('/api/config') { |req| api_handler.config_ltsv(req) }
        serv.get('/api/config.json') { |req| api_handler.config_json(req) }
      end

      if @tag
        log.debug "tag parameter is specified. Emit plugins info to '#{@tag}'"

        opts = {with_config: false, with_retry: false}
        timer_execute(:in_monitor_agent_emit, @emit_interval, repeat: true) {
          es = Fluent::MultiEventStream.new
          now = Fluent::EventTime.now
          plugins_info_all(opts).each { |record|
            es.add(now, record)
          }
          router.emit_stream(@tag, es)
        }
      end
    end

    # They are deprecated but remain for compatibility
    MONITOR_INFO = {
      'output_plugin' => ->(){ is_a?(::Fluent::Plugin::Output) },
      'buffer_queue_length' => ->(){ throw(:skip) unless instance_variable_defined?(:@buffer) && !@buffer.nil? && @buffer.is_a?(::Fluent::Plugin::Buffer); @buffer.queue.size },
      'buffer_timekeys' => ->(){ throw(:skip) unless instance_variable_defined?(:@buffer) && !@buffer.nil? && @buffer.is_a?(::Fluent::Plugin::Buffer); @buffer.timekeys },
      'buffer_total_queued_size' => ->(){ throw(:skip) unless instance_variable_defined?(:@buffer) && !@buffer.nil? && @buffer.is_a?(::Fluent::Plugin::Buffer); @buffer.stage_size + @buffer.queue_size },
      'retry_count' => ->(){ respond_to?(:num_errors) ? num_errors : nil },
    }

    def all_plugins
      array = []

      # get all input plugins
      array.concat Fluent::Engine.root_agent.inputs

      # get all output plugins
      array.concat Fluent::Engine.root_agent.outputs

      # get all filter plugins
      array.concat Fluent::Engine.root_agent.filters

      Fluent::Engine.root_agent.labels.each { |name, l|
        # TODO: Add label name to outputs / filters for identifing plugins
        array.concat l.outputs
        array.concat l.filters
      }

      array
    end

    # try to match the tag and get the info from the matched output plugin
    # TODO: Support output in label
    def plugin_info_by_tag(tag, opts={})
      matches = Fluent::Engine.root_agent.event_router.instance_variable_get(:@match_rules)
      matches.each { |rule|
        if rule.match?(tag)
          if rule.collector.is_a?(Fluent::Plugin::Output) || rule.collector.is_a?(Fluent::Output)
            return get_monitor_info(rule.collector, opts)
          end
        end
      }
      nil
    end

    # search a plugin by plugin_id
    def plugin_info_by_id(plugin_id, opts={})
      found = all_plugins.find {|pe|
        pe.respond_to?(:plugin_id) && pe.plugin_id.to_s == plugin_id
      }
      if found
        get_monitor_info(found, opts)
      else
        nil
      end
    end

    # This method returns an array because
    # multiple plugins could have the same type
    def plugins_info_by_type(type, opts={})
      array = all_plugins.select {|pe|
        (pe.config['@type'] == type) rescue nil
      }
      array.map {|pe|
        get_monitor_info(pe, opts)
      }
    end

    def plugins_info_all(opts={})
      all_plugins.map {|pe|
        get_monitor_info(pe, opts)
      }
    end

    IGNORE_ATTRIBUTES = %i(@config_root_section @config @masked_config)

    # get monitor info from the plugin `pe` and return a hash object
    def get_monitor_info(pe, opts={})
      obj = {}

      # Common plugin information
      obj['plugin_id'] = pe.plugin_id
      obj['plugin_category'] = plugin_category(pe)
      obj['type'] = pe.config['@type']
      obj['config'] = pe.config if opts[:with_config]

      # run MONITOR_INFO in plugins' instance context and store the info to obj
      MONITOR_INFO.each_pair {|key,code|
        begin
          catch(:skip) do
            obj[key] = pe.instance_exec(&code)
          end
        rescue NoMethodError => e
          unless @first_warn
            log.error "NoMethodError in monitoring plugins", key: key, plugin: pe.class, error: e
            log.error_backtrace
            @first_warn = true
          end
        rescue => e
          log.warn "unexpected error in monitoring plugins", key: key, plugin: pe.class, error: e
        end
      }

      if pe.respond_to?(:statistics)
        obj.merge!(pe.statistics.dig('output') || {})
        obj.merge!(pe.statistics.dig('filter') || {})
        obj.merge!(pe.statistics.dig('input') || {})
      end

      obj['retry'] = get_retry_info(pe.retry) if opts[:with_retry] && pe.instance_variable_defined?(:@retry)

      # include all instance variables if :with_debug_info is set
      if opts[:with_debug_info]
        iv = {}
        pe.instance_eval do
          instance_variables.each {|sym|
            next if IGNORE_ATTRIBUTES.include?(sym)
            key = sym.to_s[1..-1]  # removes first '@'
            iv[key] = instance_variable_get(sym)
          }
        end
        obj['instance_variables'] = iv
      elsif ivars = opts[:ivars]
        iv = {}
        ivars.each {|name|
          iname = "@#{name}"
          iv[name] = pe.instance_variable_get(iname) if pe.instance_variable_defined?(iname)
        }
        obj['instance_variables'] = iv
      end

      obj
    end

    RETRY_INFO = {
        'start' => '@start',
        'steps' => '@steps',
        'next_time' => '@next_time',
    }

    def get_retry_info(pe_retry)
      retry_variables = {}

      if pe_retry
        RETRY_INFO.each_pair { |key, param|
          retry_variables[key] = pe_retry.instance_variable_get(param)
        }
      end

      retry_variables
    end

    def plugin_category(pe)
      case pe
      when Fluent::Plugin::Input
        'input'
      when Fluent::Plugin::Output, Fluent::Plugin::MultiOutput, Fluent::Plugin::BareOutput
        'output'
      when Fluent::Plugin::Filter
        'filter'
      else
        'unknown'
      end
    end

    def fluentd_opts
      @fluentd_opts ||= get_fluentd_opts
    end

    def get_fluentd_opts
      opts = {}
      ObjectSpace.each_object(Fluent::Supervisor) { |obj|
        opts.merge!(obj.options)
        break
      }
      opts
    end
  end
end
