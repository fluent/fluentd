require "json"
require "fileutils"

BENCHMARK_FILE_SIZE = 1 * 1024 * 1024 * 1024
BENCHMARK_FILE_PATH = File.expand_path("./tmp/benchmark/data.log")

namespace :benchmark do
  task :init do
    # Synchronize stdout because the output order is not as intended on Windows environment
    STDOUT.sync = true
  end

  task :prepare_1GB do
    FileUtils.mkdir_p(File.dirname(BENCHMARK_FILE_PATH))
    File.open(BENCHMARK_FILE_PATH, "w") do |f|
      data = { "message": "a" * 1024 }.to_json

      loop do
        f.puts data
        break if File.size(BENCHMARK_FILE_PATH) > BENCHMARK_FILE_SIZE
      end
    end
  end

  task :show_info do
    # Output the information with markdown format
    puts "### Environment"
    puts "```"
    system "bundle exec ruby --version"
    system "bundle exec ruby bin/fluentd --version"
    puts "```\n"
  end

  desc "Run in_tail benchmark"
  task :"run:in_tail" => [:init, :prepare_1GB, :show_info] do
    # Output the results with markdown format
    puts "### in_tail with 1 GB file"
    puts "```"
    system "bundle exec ruby bin/fluentd -r ./tasks/benchmark/patch_in_tail.rb --no-supervisor -c ./tasks/benchmark/conf/in_tail.conf -o ./tmp/benchmark/fluent.log"
    puts "```"

    Rake::Task["benchmark:clean"].invoke
  end

  task :clean do
    FileUtils.rm_rf(File.dirname(BENCHMARK_FILE_PATH))
  end
end
