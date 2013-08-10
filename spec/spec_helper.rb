
require 'fluentd'
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

Fluentd.setup!

