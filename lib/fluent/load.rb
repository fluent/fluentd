# TODO: This file MUST be deleted
#       all test (and implementation) files should require its dependency by itself

require 'thread'
require 'socket'
require 'fcntl'
require 'time'
require 'monitor'
require 'stringio'
require 'fileutils'
require 'json'
require 'yajl'
require 'uri'
require 'msgpack'
require 'strptime'
begin
  require 'sigdump/setup'
rescue
  # ignore setup error on Win or similar platform which doesn't support signal
end
require 'cool.io'

require 'fluent/time'
require 'fluent/env'
require 'fluent/version'
require 'fluent/log'
require 'fluent/config'
require 'fluent/engine'
require 'fluent/rpc'
require 'fluent/mixin'
require 'fluent/process'
require 'fluent/plugin'
require 'fluent/parser'
require 'fluent/formatter'
require 'fluent/event'
require 'fluent/plugin/buffer'
require 'fluent/plugin/input'
require 'fluent/plugin/output'
require 'fluent/filter'
require 'fluent/match'
