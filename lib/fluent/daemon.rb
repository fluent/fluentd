#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

here = File.dirname(__FILE__)
$LOAD_PATH << File.expand_path(File.join(here, '..'))

require 'serverengine'
require 'fluent/env'
require 'fluent/supervisor'

server_module = Fluent.const_get(ARGV[0])
worker_module = Fluent.const_get(ARGV[1])
params = JSON.parse(ARGV[2], Fluent::DEFAULT_JSON_PARSE_OPTIONS)
ServerEngine::Daemon.run_server(server_module, worker_module) { Fluent::Supervisor.serverengine_config(params) }
