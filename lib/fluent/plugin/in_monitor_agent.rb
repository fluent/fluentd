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

require 'cool.io'

require 'fluent/input'
require 'fluent/output'
require 'fluent/filter'

module Fluent
  class MonitorAgentInput < Input
    Plugin.register_input('monitor_agent', self)

    config_param :bind, :string, default: '0.0.0.0'
    config_param :port, :integer, default: 24220
    config_param :tag, :string, default: nil
    config_param :emit_interval, :time, default: 60
    config_param :emit_config, :bool, default: false
    config_param :include_config, :bool, default: true
    config_param :include_retry, :bool, default: true

    class MonitorServlet < WEBrick::HTTPServlet::AbstractServlet
      def initialize(server, agent)
        @agent = agent
      end

      def do_GET(req, res)
        begin
          code, header, body = process(req, res)
        rescue
          code, header, body = render_json_error(500, {
              'message '=> 'Internal Server Error',
              'error' => "#{$!}",
              'backgrace'=> $!.backtrace,
            })
        end

        # set response code, header and body
        res.status = code
        header.each_pair {|k,v|
          res[k] = v
        }
        res.body = body
      end

      def build_object(req, res)
        unless req.path_info == ""
          return render_json_error(404, "Not found")
        end

        # parse ?=query string
        if req.query_string
          begin
            qs = CGI.parse(req.query_string)
          rescue
            return render_json_error(400, "Invalid query string")
          end
        else
          qs = Hash.new {|h,k| [] }
        end

        # if ?debug=1 is set, set :with_debug_info for get_monitor_info
        # and :pretty_json for render_json_error
        opts = {with_config: @agent.include_config, with_retry: @agent.include_retry}
        if s = qs['debug'] and s[0]
          opts[:with_debug_info] = true
          opts[:pretty_json] = true
        end

        if ivars = (qs['with_ivars'] || []).first
          opts[:ivars] = ivars.split(',')
        end

        if with_config = get_search_parameter(qs, 'with_config'.freeze)
          opts[:with_config] = Fluent::Config.bool_value(with_config)
        end

        if with_retry = get_search_parameter(qs, 'with_retry'.freeze)
          opts[:with_retry] = Fluent::Config.bool_value(with_retry)
        end

        if tag = get_search_parameter(qs, 'tag'.freeze)
          # ?tag= to search an output plugin by match pattern
          if obj = @agent.plugin_info_by_tag(tag, opts)
            list = [obj]
          else
            list = []
          end

        elsif plugin_id = get_search_parameter(qs, '@id'.freeze)
          # ?@id= to search a plugin by 'id <plugin_id>' config param
          if obj = @agent.plugin_info_by_id(plugin_id, opts)
            list = [obj]
          else
            list = []
          end

        elsif plugin_id = get_search_parameter(qs, 'id'.freeze)
          # Without @ version of ?@id= for backward compatibility
          if obj = @agent.plugin_info_by_id(plugin_id, opts)
            list = [obj]
          else
            list = []
          end

        elsif plugin_type = get_search_parameter(qs, '@type'.freeze)
          # ?@type= to search plugins by 'type <type>' config param
          list = @agent.plugins_info_by_type(plugin_type, opts)

        elsif plugin_type = get_search_parameter(qs, 'type'.freeze)
          # Without @ version of ?@type= for backward compatibility
          list = @agent.plugins_info_by_type(plugin_type, opts)

        else
          # otherwise show all plugins
          list = @agent.plugins_info_all(opts)
        end

        return list, opts
      end

      def get_search_parameter(qs, param_name)
        return nil unless qs.has_key?(param_name)
        qs[param_name].first
      end

      def render_json(obj, opts={})
        render_json_error(200, obj, opts)
      end

      def render_json_error(code, obj, opts={})
        if opts[:pretty_json]
          js = JSON.pretty_generate(obj)
        else
          js = obj.to_json
        end
        [code, {'Content-Type'=>'application/json'}, js]
      end
    end

    class LTSVMonitorServlet < MonitorServlet
      def process(req, res)
        list, opts = build_object(req, res)
        return unless list

        normalized = JSON.parse(list.to_json)

        text = ''

        normalized.map {|hash|
          row = []
          hash.each_pair {|k,v|
            unless v.is_a?(Hash) || v.is_a?(Array)
              row << "#{k}:#{v}"
            end
          }
          text << row.join("\t") << "\n"
        }

        [200, {'Content-Type'=>'text/plain'}, text]
      end
    end

    class JSONMonitorServlet < MonitorServlet
      def process(req, res)
        list, opts = build_object(req, res)
        return unless list

        render_json({
            'plugins' => list
          }, opts)
      end
    end

    class ConfigMonitorServlet < MonitorServlet
      def build_object(req, res)
        {
          'pid' => Process.pid,
          'ppid' => Process.ppid
        }.merge(@agent.fluentd_opts)
      end
    end

    class LTSVConfigMonitorServlet < ConfigMonitorServlet
      def process(req, res)
        result = build_object(req, res)

        row = []
        JSON.parse(result.to_json).each_pair { |k, v|
          row << "#{k}:#{v}"
        }
        text = row.join("\t")

        [200, {'Content-Type'=>'text/plain'}, text]
      end
    end

    class JSONConfigMonitorServlet < ConfigMonitorServlet
      def process(req, res)
        result = build_object(req, res)
        render_json(result)
      end
    end

    class TimerWatcher < Coolio::TimerWatcher
      def initialize(interval, log, &callback)
        @callback = callback
        @log = log

        # Avoid long shutdown time
        @num_call = 0
        if interval >= 10
          min_interval = 10
          @call_interval = interval / 10
        else
          min_interval = interval
          @call_interval = 0
        end

        super(min_interval, true)
      end

      def on_timer
        @num_call += 1
        if @num_call >= @call_interval
          @num_call = 0
          @callback.call
        end
      rescue => e
        @log.error e.to_s
        @log.error_backtrace
      end
    end

    def start
      log.debug "listening monitoring http server on http://#{@bind}:#{@port}/api/plugins"
      @srv = WEBrick::HTTPServer.new({
          BindAddress: @bind,
          Port: @port,
          Logger: WEBrick::Log.new(STDERR, WEBrick::Log::FATAL),
          AccessLog: [],
        })
      @srv.mount('/api/plugins', LTSVMonitorServlet, self)
      @srv.mount('/api/plugins.json', JSONMonitorServlet, self)
      @srv.mount('/api/config', LTSVConfigMonitorServlet, self)
      @srv.mount('/api/config.json', JSONConfigMonitorServlet, self)
      @thread = Thread.new {
        @srv.start
      }
      if @tag
        log.debug "tag parameter is specified. Emit plugins info to '#{@tag}'"

        @loop = Coolio::Loop.new
        opts = {with_config: @emit_config, with_retry: false}
        timer = TimerWatcher.new(@emit_interval, log) {
          es = MultiEventStream.new
          now = Engine.now
          plugins_info_all(opts).each { |record|
            es.add(now, record)
          }
          router.emit_stream(@tag, es)
        }
        @loop.attach(timer)
        @thread_for_emit = Thread.new(&method(:run))
      end
    end

    def run
      @loop.run
    rescue => e
      log.error "unexpected error", error: e.to_s
      log.error_backtrace
    end

    def shutdown
      if @srv
        @srv.shutdown
        @srv = nil
      end
      if @thread
        @thread.join
        @thread = nil
      end
      if @tag
        @loop.watchers.each { |w| w.detach }
        @loop.stop
        @loop = nil
        @thread_for_emit.join
        @thread_for_emit = nil
      end
    end

    MONITOR_INFO = {
      'output_plugin' => 'is_a?(::Fluent::Output)', # deprecated. Use plugin_category instead
      'buffer_queue_length' => '@buffer.queue_size',
      'buffer_total_queued_size' => '@buffer.total_queued_chunk_size',
      'retry_count' => '@num_errors',
    }

    def all_plugins
      array = []

      # get all input plugins
      array.concat Engine.root_agent.inputs

      # get all output plugins
      Engine.root_agent.outputs.each { |o|
        MonitorAgentInput.collect_children(o, array)
      }
      # get all filter plugins
      Engine.root_agent.filters.each { |f|
        MonitorAgentInput.collect_children(f, array)
      }
      Engine.root_agent.labels.each { |name, l|
        # TODO: Add label name to outputs / filters for identifing plugins
        l.outputs.each { |o| MonitorAgentInput.collect_children(o, array) }
        l.filters.each { |f| MonitorAgentInput.collect_children(f, array) }
      }

      array
    end

    # get nexted plugins (such as <store> of the copy plugin)
    # from the plugin `pe` recursively
    def self.collect_children(pe, array=[])
      array << pe
      if pe.is_a?(MultiOutput) && pe.respond_to?(:outputs)
        pe.outputs.each {|nop|
          collect_children(nop, array)
        }
      end
      array
    end

    # try to match the tag and get the info from the matched output plugin
    # TODO: Support output in label
    def plugin_info_by_tag(tag, opts={})
      matches = Engine.root_agent.event_router.instance_variable_get(:@match_rules)
      matches.each { |rule|
        if rule.match?(tag)
          if rule.collector.is_a?(Output)
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
        (pe.config['@type'] == type || pe.config['type'] == type) rescue nil
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

    # TODO: use %i() after drop ruby v1.9.3 support.
    IGNORE_ATTRIBUTES = %W(@config_root_section @config @masked_config).map(&:to_sym)
    EMPTY_RESULT = {}

    # get monitor info from the plugin `pe` and return a hash object
    def get_monitor_info(pe, opts={})
      obj = {}

      # Common plugin information
      obj['plugin_id'] = pe.plugin_id
      obj['plugin_category'] = plugin_category(pe)
      obj['type'] = pe.config['@type'] || pe.config['type']
      obj['config'] = pe.config if opts[:with_config]

      # run MONITOR_INFO in plugins' instance context and store the info to obj
      MONITOR_INFO.each_pair {|key,code|
        begin
          obj[key] = pe.instance_eval(code)
        rescue
        end
      }

      if opts[:with_retry]
        num_errors = pe.instance_variable_get(:@num_errors)
        if num_errors
          obj['retry'] = num_errors.zero? ? EMPTY_RESULT : get_retry_info(pe, num_errors)
        end
      end

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

    def get_retry_info(pe, num_errors)
      retry_variables = {}
      retry_variables['steps'] = num_errors
      retry_variables['next_time'] = Time.at(pe.instance_variable_get('@next_retry_time'.freeze))
      retry_variables
    end

    def plugin_category(pe)
      case pe
      when Fluent::Input
        'input'.freeze
      when Fluent::Output
        'output'.freeze
      when Fluent::Filter
        'filter'.freeze
      else
        'unknown'.freeze
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
