#
# Fluent
#
# Copyright (C) 2011 FURUHASHI Sadayuki
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

class MonitorAgentInput < Input
  Plugin.register_input('monitor_agent', self)

  require 'webrick'

  def initialize
    require 'cgi'
    super
  end

  config_param :bind, :string, :default => '0.0.0.0'
  config_param :port, :integer, :default => 24220

  class MonitorServlet < WEBrick::HTTPServlet::AbstractServlet
    def initialize(server, agent)
      @agent = agent
    end

    def do_GET(req, res)
      begin
        code, header, body = process_GET(req, res)
      rescue
        code, header, body = render_json_error(500, {
          'message '=> 'Internal Server Error',
          'error' => "#{$!}",
          'backgrace'=> $!.backtrace,
        })
      end

      res.status = code
      header.each_pair {|k,v|
        res[k] = v
      }
      res.body = body
    end

    def process_GET(req, res)
      unless req.path_info == "/api/plugins"
        return [404, {'Content-Type'=>'text/plain'}, '404 Not found']
      end

      if req.query_string
        begin
          qs = CGI.parse(req.query_string)
        rescue
          return render_json_error(400, "Invalid query string")
        end
      else
        qs = {}
      end

      if tags = qs['tag'] and tag = tags[0]
        if obj = @agent.plugin_info_by_tag(tag)
          list = [obj]
        else
          list = []
        end

      elsif plugin_ids = qs['id'] and plugin_id = plugin_ids[0]
        if obj = @agent.plugin_info_by_id(plugin_id)
          list = [obj]
        else
          list = []
        end

      elsif types = qs['type'] and type = types[0]
        list = @agent.plugins_info_by_type(type)

      else
        list = @agent.plugins_info_all
      end

      render_json({
        'plugins' => list
      })
    end

    def render_json(obj)
      render_json_error(200, obj)
    end

    def render_json_error(code, obj)
      if @agent.debug_mode?
        js = JSON.pretty_generate(obj)
      else
        js = obj.to_json
        js = JSON.pretty_generate(obj)
      end
      [code, {'Content-Type'=>'application/json'}, js]
    end
  end

  def configure(conf)
    if conf['debug']
      @debug = true
    else
      @debug = false
    end
    super
  end

  def debug_mode?
    @debug
  end

  def start
    @srv = WEBrick::HTTPServer.new({
      :BindAddress => @bind,
      :Port => @port,
      :AccessLog => [],
    })
    @srv.mount('/', MonitorServlet, self)
    @thread = Thread.new {
      @srv.start
    }
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
  end

  MONITOR_INFO = {
    'plugin_id' => 'plugin_id',
    'type' => 'config["type"]',
    'buffer_queue_size' => '@buffer.queue_size',
    'retry_count' => '@error_history.size',
    'config' => 'config',
  }

  def self.collect_children(pe, array=[])
    array << pe
    if pe.is_a?(MultiOutput) && pe.respond_to?(:outputs)
      pe.outputs.each {|nop|
        collect_children(nop, array)
      }
    end
    array
  end

  def all_plugins
    array = []

    array.concat Engine.sources

    Engine.matches.each {|m|
      MonitorAgentInput.collect_children(m.output, array)
    }

    array
  end

  def plugins_info_all
    all_plugins.map {|pe|
      get_monitor_info(pe)
    }
  end

  def plugin_info_by_tag(tag)
    m = Engine.match(tag)
    if m
      pe = m.output
      get_monitor_info(pe)
    else
      nil
    end
  end

  def plugin_info_by_id(plugin_id)
    found = all_plugins.find {|pe|
      pe.respond_to?(:plugin_id) && pe.plugin_id.to_s == plugin_id
    }
    if found
      get_monitor_info(pe)
    else
      nil
    end
  end

  def plugins_info_by_type(type)
    array = all_plugins.select {|pe|
      pe.config['type'] == type rescue nil
    }
    array.map {|pe|
      get_monitor_info(pe)
    }
  end

  def get_monitor_info(pe)
    obj = {}
    MONITOR_INFO.each_pair {|key,code|
      begin
        obj[key] = pe.instance_eval(code)
      rescue
      end
    }

    if debug_mode?
      # include all instance variables
      iv = {}
      pe.instance_eval do
        instance_variables.each {|k|
          iv[k.to_s[1..-1]] = instance_variable_get(k)
        }
      end
      obj['instance_variables'] = iv
    end

    obj
  end
end

end
