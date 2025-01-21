Fluentd: Open-Source Log Collector
===================================

[![Testing on Ubuntu](https://github.com/fluent/fluentd/actions/workflows/linux-test.yaml/badge.svg?branch=master)](https://github.com/fluent/fluentd/actions/workflows/linux-test.yaml)
[![Testing on Windows](https://github.com/fluent/fluentd/actions/workflows/windows-test.yaml/badge.svg?branch=master)](https://github.com/fluent/fluentd/actions/workflows/windows-test.yaml)
[![Testing on macOS](https://github.com/fluent/fluentd/actions/workflows/macos-test.yaml/badge.svg?branch=master)](https://github.com/fluent/fluentd/actions/workflows/macos-test.yaml)
[![Code Climate](https://codeclimate.com/github/fluent/fluentd/badges/gpa.svg)](https://codeclimate.com/github/fluent/fluentd)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/1189/badge)](https://bestpractices.coreinfrastructure.org/projects/1189)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/fluent/fluentd/badge)](https://scorecard.dev/viewer/?uri=github.com/fluent/fluentd)

[Fluentd](https://www.fluentd.org/) collects events from various data sources and writes them to files, RDBMS, NoSQL, IaaS, SaaS, Hadoop and so on. Fluentd helps you unify your logging infrastructure (Learn more about the [Unified Logging Layer](https://www.fluentd.org/blog/unified-logging-layer)).

<p align="center">
<img src="https://www.fluentd.org/images/fluentd-architecture.png" width="500px"/>
</p>

## Quick Start

    $ gem install fluentd
    $ fluentd -s conf
    $ fluentd -c conf/fluent.conf &
    $ echo '{"json":"message"}' | fluent-cat debug.test

## Development

### Branch

- master: For v1 development.
- v0.12: For v0.12. This is deprecated version. we already stopped supporting (See https://www.fluentd.org/blog/drop-schedule-announcement-in-2019).

### Prerequisites

- Ruby 3.2 or later
- git

`git` should be in `PATH`. On Windows, you can use `Github for Windows` and `GitShell` for easy setup.

### Install dependent gems

Use bundler:

    $ gem install bundler
    $ bundle install --path vendor/bundle

### Run test

    $ bundle exec rake test

You can run specified test via `TEST` environment variable:

    $ bundle exec rake test TEST=test/test_specified_path.rb
    $ bundle exec rake test TEST=test/test_*.rb

## More Information

- Website: https://www.fluentd.org/
- Documentation: https://docs.fluentd.org/
- Project repository: https://github.com/fluent
- Discussion: https://github.com/fluent/fluentd/discussions
- Slack / Community: https://slack.fluentd.org
- Newsletters: https://www.fluentd.org/newsletter
- Author: [Sadayuki Furuhashi](https://github.com/frsyuki)
- Copyright: 2011-2021 Fluentd Authors
- License: Apache License, Version 2.0

## Security

A third party security audit was performed by Cure53, you can see the full report [here](docs/SECURITY_AUDIT.pdf).

See [SECURITY](SECURITY.md) to contact us about vulnerability.

## Contributors:

Patches contributed by [great developers](https://github.com/fluent/fluentd/contributors).
