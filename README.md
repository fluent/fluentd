Fluentd: Open-Source Log Collector
===================================

[<img src="https://travis-ci.org/fluent/fluentd.svg" />](https://travis-ci.org/fluent/fluentd) [![Code Climate](https://codeclimate.com/github/fluent/fluentd/badges/gpa.svg)](https://codeclimate.com/github/fluent/fluentd)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/1189/badge)](https://bestpractices.coreinfrastructure.org/projects/1189)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Ffluent%2Ffluentd.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Ffluent%2Ffluentd?ref=badge_shield)

[Fluentd](https://www.fluentd.org/) collects events from various data sources and writes them to files, RDBMS, NoSQL, IaaS, SaaS, Hadoop and so on. Fluentd helps you unify your logging infrastructure (Learn more about the [Unified Logging Layer](https://www.fluentd.org/blog/unified-logging-layer)).

<p align="center">
<img src="https://docs.fluentd.org/images/fluentd-architecture.png" width="500px"/>
</p>

An event consists of *tag*, *time* and *record*. Tag is a string separated with '.' (e.g. myapp.access). It is used to categorize events. Time is a UNIX time recorded at occurrence of an event. Record is a JSON object.

## Example Use Cases

Use Case | Description | Diagram
-------- | ------------|:---------:
Centralizing Apache/Nginx Server Logs | Fluentd can be used to tail access/error logs and transport them reliably to remote systems. | <img src="https://www.fluentd.org/assets/img/recipes/elasticsearch-s3-fluentd.png" height="150"/>
Syslog Alerting | Fluentd can "grep" for events and send out alerts. | <img src="https://www.fluentd.org/images/syslog-fluentd-alert.png" height="100"/>
Mobile/Web Application Logging | Fluentd can function as middleware to enable asynchronous, scalable logging for user action events. | <img src="https://www.fluentd.org/assets/img/datasources/asynchronous_logging.png" height="150"/>

## Quick Start

    $ gem install fluentd
    $ fluentd -s conf
    $ fluentd -c conf/fluent.conf &
    $ echo '{"json":"message"}' | fluent-cat debug.test

## Development

### Prerequisites

- Ruby 2.1 or later
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

## Running in Production

Many enterprises run Fluentd in production to handle all of their logging needs. For enterprises requiring Security tested binaries, SLA-based support, architectural guidance, and enhanced plugin connectors see [Fluentd Enterprise](https://www.fluentd.org/enterprise).


## Fluentd UI: Admin GUI

[Fluentd UI](https://github.com/fluent/fluentd-ui) is a graphical user interface to start/stop/configure Fluentd.

<p align="center"><img width="500" src="https://www.fluentd.org/images/blog/fluentd-ui.gif"/></p>

## More Information

- Website: https://www.fluentd.org/
- Documentation: https://docs.fluentd.org/
- Project repository: https://github.com/fluent
- Discussion: https://groups.google.com/group/fluentd
- Slack / Community: https://slack.fluentd.org
- Newsletters: https://www.fluentd.org/newsletter_signup
- Author: Sadayuki Furuhashi
- Copyright: 2011-2018 Fluentd Authors
- License: Apache License, Version 2.0

## Contributors:

Patches contributed by [great developers](https://github.com/fluent/fluentd/contributors).

[<img src="https://ga-beacon.appspot.com/UA-24890265-6/fluent/fluentd" />](https://github.com/fluent/fluentd)
