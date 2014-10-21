Fluentd: Open-Source Log Collector
===================================

[<img src="https://travis-ci.org/fluent/fluentd.svg" />](https://travis-ci.org/fluent/fluentd) [![Code Climate](https://codeclimate.com/github/fluent/fluentd/badges/gpa.svg)](https://codeclimate.com/github/fluent/fluentd)

[Fluentd](http://fluentd.org/) collects events from various data sources and writes them to files, RDBMS, NoSQL, IaaS, SaaS, Hadoop and so on. Fluentd helps you unify your logging infrastructure (Learn more about the [Unified Logging Layer](http://www.fluentd.org/blog/unified-logging-layer)).

<p align="center">
<img src="http://docs.fluentd.org/images/fluentd-architecture.png" width="500px"/>
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

## Fluentd UI: Admin GUI

[Fluentd UI](https://github.com/fluent/fluentd-ui) is a graphical user interface to start/stop/configure Fluentd.

<p align="center"><img width="500" src="http://www.fluentd.org/images/blog/fluentd-ui.gif"/></p>

## More Information

- Website: http://fluentd.org/
- Documentation: http://docs.fluentd.org/
- Source repository: http://github.com/fluent
- Discussion: http://groups.google.com/group/fluentd
- Newsletters: http://get.treasuredata.com/Fluentd_education
- Author: Sadayuki Furuhashi
- Copyright: (c) 2011 FURUHASHI Sadayuki
- License: Apache License, Version 2.0

## Contributors:

Patches contributed by [great developers](https://github.com/fluent/fluentd/contributors).

[<img src="https://ga-beacon.appspot.com/UA-24890265-6/fluent/fluentd" />](https://github.com/fluent/fluentd)

