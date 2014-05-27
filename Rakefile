#!/usr/bin/env rake
require "bundler/gem_tasks"

require 'fileutils'
require 'rake/testtask'
require 'rake/clean'
require 'rspec/core'
require 'rspec/core/rake_task'

task :test => [:base_test, :spec]

desc 'Run test_unit based test'
Rake::TestTask.new(:base_test) do |t|
  t.libs << "test"
  t.test_files = (Dir["test/test_*.rb"] + Dir["test/plugin/test_*.rb"] - ["helper.rb"]).sort
  t.verbose = true
  #t.warning = true
end

task :parallel_test do
  FileUtils.rm_rf('./test/tmp')
  sh("parallel_test ./test/*.rb ./test/plugin/*.rb")
  FileUtils.rm_rf('./test/tmp')
end

desc 'Run rspec based test'
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = %w[-c -f progress -r ./spec/spec_helper.rb]
  t.pattern = 'spec/**/*_spec.rb'
  t.verbose = true
end

desc 'Run rspec with simplecov'
task :coverage do |t|
  ENV['SIMPLE_COV'] = '1'
  Rake::Task["spec"].invoke
end

task :default => [:test, :build]
