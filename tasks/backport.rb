require_relative 'backport/backporter'

=begin

When you want to manually execute backporting, set the following
environment variables:

* GITHUB_REPOSITORY: fluent/fluentd
* GITHUB_TOKEN: ${PERSONAL_ACCESS_TOKEN}

Optional:

* REPOSITORY_REMOTE: origin
  If you execute in forked repository, it might be 'upstream'

=end

def append_additional_arguments(commands)
  if ENV['DRY_RUN']
    commands << '--dry-run'
  end
  if ENV['GITHUB_REPOSITORY']
    commands << '--upstream'
    commands << ENV['GITHUB_REPOSITORY']
  end
  if ENV['REPOSITORY_REMOTE']
    commands << '--remote'
    commands << ENV['REPOSITORY_REMOTE']
  end
  commands
end

namespace :backport do

  desc "Backport PR to v1.16 branch"
  task :v1_16 do
    backporter = PullRequestBackporter.new
    commands = ['--branch', 'v1.16', '--log-level', 'debug']
    commands = append_additional_arguments(commands)
    eixt(backporter.run(commands))
  end

  desc "Backport PR to v1.19 branch"
  task :v1_19 do
    commands = ['--branch', 'v1.19', '--log-level', 'debug']
    commands = append_additional_arguments(commands)
    backporter = PullRequestBackporter.new
    exit(backporter.run(commands))
  end
end
