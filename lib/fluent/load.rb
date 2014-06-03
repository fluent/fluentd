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
begin
  require 'sigdump/setup'
rescue
  # ignore setup error on Win or similar platform which doesn't support signal
end
# I hate global variable but we suffer pain now for the sake of future.
# We will switch to MessagePack 0.5 and deprecate 0.4.
$use_msgpack_5 = defined?(MessagePack::Packer) ? true : false
require 'cool.io'
require 'fluent/env'
require 'fluent/version'
require 'fluent/log'
require 'fluent/status'
require 'fluent/config'
require 'fluent/engine'
require 'fluent/mixin'
require 'fluent/process'
require 'fluent/plugin'
require 'fluent/parser'
require 'fluent/formatter'
require 'fluent/event'
require 'fluent/buffer'
require 'fluent/input'
require 'fluent/output'
require 'fluent/match'
