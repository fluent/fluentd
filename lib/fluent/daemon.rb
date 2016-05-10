#!/usr/bin/env ruby
# -*- coding: utf-8 -*-

here = File.dirname(__FILE__)
$LOAD_PATH << File.expand_path(File.join(here, '..'))

require 'serverengine'
require 'fluent/supervisor'

server_module = Fluent.const_get(ARGV[0])
worker_module = Fluent.const_get(ARGV[1])
# it doesn't call ARGV in block because when reloading config, params will be initialized and then it can't use previous config.
config_path = ARGV[2]
params = JSON.parse(ARGV[3])
ServerEngine::Daemon.run_server(server_module, worker_module) { Fluent::Supervisor.load_config(config_path, params) }
