#!/usr/bin/env rake
require "bundler/gem_tasks"

require 'fileutils'
require 'rake/testtask'
require 'rake/clean'

task test: [:base_test]

# 1. update ChangeLog and lib/fluent/version.rb
# 2. bundle && bundle exec rake build:all
# 3. release 3 packages built on pkg/ directory
namespace :build do
  desc 'Build gems for all platforms'
  task :all do
    Bundler.with_clean_env do
      %w[ruby x86-mingw32 x64-mingw32].each do |name|
        ENV['GEM_BUILD_FAKE_PLATFORM'] = name
        Rake::Task["build"].execute
      end
    end
  end
end

desc 'Run test_unit based test'
Rake::TestTask.new(:base_test) do |t|
  # To run test with dumping all test case names (to find never ending test case)
  #  $ bundle exec rake test TESTOPTS=-v
  #
  # To run test for only one file (or file path pattern)
  #  $ bundle exec rake base_test TEST=test/test_specified_path.rb
  #  $ bundle exec rake base_test TEST=test/test_*.rb
  t.libs << "test"
  t.test_files = if ENV["WIN_RAPID"]
                   ["test/test_event.rb", "test/test_supervisor.rb", "test/plugin_helper/test_event_loop.rb"]
                 else
                   Dir["test/**/test_*.rb"].sort
                 end
  t.verbose = true
  t.warning = true
  t.ruby_opts = ["-Eascii-8bit:ascii-8bit"]
end

task :parallel_test do
  FileUtils.rm_rf('./test/tmp')
  sh("parallel_test ./test/*.rb ./test/plugin/*.rb")
  FileUtils.rm_rf('./test/tmp')
end

desc 'Run test with simplecov'
task :coverage do |t|
  ENV['SIMPLE_COV'] = '1'
  Rake::Task["test"].invoke
end

task default: [:test, :build]
