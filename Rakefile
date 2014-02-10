#!/usr/bin/env rake
require "bundler/gem_tasks"

require 'fileutils'
require 'rake/testtask'
require 'rake/clean'

task :test => [:base_test]

Rake::TestTask.new(:base_test) do |t|
  t.libs << "test"
  t.test_files = (Dir["test/*.rb"] + Dir["test/plugin/*.rb"] - ["helper.rb"]).sort
  t.verbose = true
  #t.warning = true
end

task :parallel_test do
  FileUtils.rm_rf('./test/tmp')
  sh("parallel_test ./test/*.rb ./test/plugin/*.rb")
  FileUtils.rm_rf('./test/tmp')
end

task :default => [:test, :build]
