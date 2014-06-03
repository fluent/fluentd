Fluentd: Windows branch
=======================

This is a branch version to make fluentd work on Windows!

**This is a very preliminary version, and we expect you would run into a lot of errors.
If you have any feedback, please let us know!**

## Gem

You can use a local gem, pkg/fluentd-0.10.46.gem included.

    gem install pkg/fluentd-0.10.46.gem

## General condition

### Setting environment
Make settings just like as original one.(RUBY_INSTALL_DIR in env.rb is not required any more.)

### Daemon (Windows Service)
Implementation exists, but some technical problems in Ruby and Coolio on Windows. So we cannot announce how to use yet. Sorry.

### Termination of  fluentd
It cannot be terminatd by the usual Ctrl+C yet. For now, you can terminate it by closing the command prompt window.

## Plugin condition

### input plugin

| name | condition |
|:-----    |:----------|
|in_http   |Looks good.|
|in_forword|Looks good|
|in_tail   |Looks good, but NTFS requried, Network drive is not supported, Possibility of problems are on old Windows versions(we need check). FILE_SHARE_READ on a target file is absolutely required.|
|in_exec   | Looks good.|


### output plugin
| name | condition |
|:-----|:----------|
|out_copy|Looks good.|
|out_stdout|Looks good.|
|out_null|Looks good.|
|out_forword|Looks good, but send_timeout option cannot be used.|
|out_file|Looks good.|
|out_exec|Looks good.|
|out_exec_filter|Not good. Error occures when stopping fluentd.|
|out_roundrobin|Looks good.|

## buffer plugin 
| name | condition |
|:-----|:----------|
| buf_memory | Looks good.|
|buf_file|Looks good.|

-----------------------------------------------------


Fluentd: Open-Source Data Collector
===================================

[<img src="https://travis-ci.org/fluent/fluentd.png" />](https://travis-ci.org/fluent/fluentd) [<img src="https://codeclimate.com/github/fluent/fluentd.png " />](https://codeclimate.com/github/fluent/fluentd)


[Fluentd](http://fluentd.org/) collects events from various data sources and writes them to files, database or other types of storages. You can simplify your data stream, and have robust data collection mechanism instantly:

<p align="center">
<img src="http://docs.fluentd.org/images/fluentd-architecture.png" width="500px"/>
</p>

An event consists of *tag*, *time* and *record*. Tag is a string separated with '.' (e.g. myapp.access). It is used to categorize events. Time is a UNIX time recorded at occurrence of an event. Record is a JSON object.


## Quick Start

    $ gem install fluentd
    $ fluentd -s conf
    $ fluentd -c conf/fluent.conf &
    $ echo '{"json":"message"}' | fluent-cat debug.test

## More Information

- Web site:  http://fluentd.org/
- Documents: http://docs.fluentd.org/
- Source repository: http://github.com/fluent
- Discussion: http://groups.google.com/group/fluentd
- News Letters: http://go.treasuredata.com/Fluentd_education
- Author: Sadayuki Furuhashi
- Copyright: (c) 2011 FURUHASHI Sadayuki
- License: Apache License, Version 2.0

## Contributors:

Patches contributed by [great developers](https://github.com/fluent/fluentd/contributors).

[<img src="https://ga-beacon.appspot.com/UA-24890265-6/fluent/fluentd" />](https://github.com/fluent/fluentd)

