require 'rake'
require 'rake/testtask'
require 'rake/clean'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "fluentd"
    gemspec.summary = "Fluent event collector"
    gemspec.author = "Sadayuki Furuhashi"
    gemspec.email = "frsyuki@gmail.com"
    gemspec.homepage = "http://fluentd.org/"
    gemspec.has_rdoc = false
    gemspec.require_paths = ["lib"]
    gemspec.add_dependency "msgpack", "~> 0.4.4"
    gemspec.add_dependency "json", ">= 1.4.3"
    gemspec.add_dependency "yajl-ruby", "~> 1.0"
    gemspec.add_dependency "cool.io", "~> 1.1.0"
    gemspec.add_dependency "http_parser.rb", "~> 0.5.1"
    gemspec.add_development_dependency "rake", ">= 0.9.2"
    gemspec.add_development_dependency "rr", ">= 1.0.0"
    gemspec.add_development_dependency "timecop", ">= 0.3.0"
    gemspec.add_development_dependency "jeweler", ">= 1.0.0"
    gemspec.test_files = Dir["test/**/*.rb"]
    gemspec.files = Dir["bin/**/*", "lib/**/*", "test/**/*.rb"] +
      %w[fluent.conf VERSION AUTHORS Rakefile COPYING fluentd.gemspec Gemfile]
    gemspec.executables = ['fluentd', 'fluent-cat', 'fluent-gem']
    gemspec.required_ruby_version = '~> 1.9.2'
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

VERSION_FILE = "lib/fluent/version.rb"

file VERSION_FILE => ["VERSION"] do |t|
  version = File.read("VERSION").strip
  File.open(VERSION_FILE, "w") {|f|
    f.write <<EOF
module Fluent

VERSION = '#{version}'

end
EOF
  }
end

task :test => [:base_test]

Rake::TestTask.new(:base_test) do |t|
  t.libs << "test"
  t.test_files = (Dir["test/*.rb"] + Dir["test/plugin/*.rb"] - ["helper.rb"]).sort
  t.verbose = true
  #t.warning = true
end

# workaround for fluentd >= 0 dependency
task :mv_gemfile do
  File.rename "Gemfile", "Gemfile.bak" rescue nil
end

# workaround for fluentd >= 0 dependency
task :revert_gemfile do
  File.rename "Gemfile.bak", "Gemfile" rescue nil
end

task :default => [VERSION_FILE, :mv_gemfile, :build, :revert_gemfile]

