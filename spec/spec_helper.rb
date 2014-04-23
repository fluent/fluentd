
require 'fluent/load'
require 'fileutils'

src_root = File.expand_path('..', File.dirname(__FILE__))
$LOAD_PATH << File.join(src_root, "lib")
$LOAD_PATH << src_root

if ENV['SIMPLE_COV']
  require 'simplecov'
  if defined?(SimpleCov::SourceFile)
    mod = SimpleCov::SourceFile
    def mod.new(*args, &block)
      m = allocate
      m.instance_eval do
        begin
          initialize(*args, &block)
        rescue Encoding::UndefinedConversionError
          @src = "".force_encoding('UTF-8')
        end
      end
      m
    end
  end
  SimpleCov.start do
    add_filter 'spec/'
    add_filter 'pkg/'
    add_filter 'vendor/'
  end
end

if ENV['GC_STRESS']
  STDERR.puts "enable GC.stress"
  GC.stress = true
end

require 'fluent/log'
require 'fluent/plugin'

class DummyLogDevice
  attr_reader :logs
  def initialize ; @logs = [] ; end
  def tty? ; false ; end
  def puts(*args) ; args.each{ |arg| write(arg + "\n") } ; end
  def write(message) ; @logs.push message ; end
  def flush ; true ; end
  def close ; true ; end
end

class TestLogger < Fluent::PluginLogger
  def initialize
    @logdev = DummyLogDevice.new
    super(Fluent::Log.new(@logdev))
  end
  def logs
    @logdev.logs
  end
end

$log ||= TestLogger.new
