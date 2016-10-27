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

desc 'Build Coverity tarball & upload it'
task :coverity do
  # https://scan.coverity.com/projects/fluentd?tab=overview
  # See "View Defects" after sign-in.
  #
  # Setup steps:
  # 1. get coverity build tool and set PATH to bin/: https://scan.coverity.com/download
  # 2. set environment variables:
  #    * $COVERITY_USER (your email address)
  #    * $COVERITY_TOKEN (token for Fluentd project: https://scan.coverity.com/projects/fluentd?tab=project_settings)
  sh "cov-build --dir cov-int --no-command --fs-capture-search ./"
  sh "tar czf cov-fluentd.tar.gz cov-int"
  user = ENV['COVERITY_USER']
  token = ENV['COVERITY_TOKEN']
  sh "curl --form token=#{token} --form email=#{user} --form file=@cov-fluentd.tar.gz --form version=\"Master\" --form description=\"GIT Master\" https://scan.coverity.com/builds?project=Fluentd"
  FileUtils.rm_rf(['./cov-int', 'cov-fluentd.tar.gz'])
end

task default: [:test, :build]
