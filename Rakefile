#!/usr/bin/env rake
require "bundler/gem_tasks"

require 'fileutils'
require 'rake/testtask'
require 'rake/clean'

task :test => [:base_test, :spec]

Rake::TestTask.new(:base_test) do |t|
  t.libs << "test"
  t.test_files = (Dir["test/*.rb"] + Dir["test/plugin/*.rb"] - ["helper.rb"]).sort
  t.verbose = true
  #t.warning = true
end

require 'rspec/core'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
end

task :parallel_test do
  FileUtils.rm_rf('./test/tmp')
  sh("parallel_test ./test/*.rb ./test/plugin/*.rb")
  FileUtils.rm_rf('./test/tmp')
end

task :default => [:test, :build]
