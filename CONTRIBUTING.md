# Contributing to Fluentd

We'd love your contribution. Here are the guidelines!

## Got a question or problem?

RESOURCES of [Official site](https://www.fluentd.org/) and [Fluentd documentation](https://docs.fluentd.org/) may help you.

If you have further questions about Fluentd and plugins, please direct these to [Mailing List](https://groups.google.com/forum/#!forum/fluentd).
Don't use Github issue for asking questions. Here are examples:

- I installed xxx plugin but it doesn't work. Why?
- Fluentd starts but logs are not sent to xxx. Am I wrong?
- I want to do xxx. How to realize it with plugins?

We may close such questions to keep clear repository for developers and users.
Github issue is mainly for submitting a bug report or feature request. See below.

If you can't judge your case is a bug or not, use mailing list or slack first.

## Found a bug?

If you find a bug of Fluentd or a mistake in the documentation, you can help us by
submitting an issue to Fluentd. Even better you can submit a Pull Request with a fix.

* **Fluentd**: Use [fluentd](https://github.com/fluent/fluentd) repository. Fill issue template.
* **Documentation**: Use [fluentd-docs](https://github.com/fluent/fluentd-docs) repository.

If you find a bug of 3rd party plugins, please submit an issue to each plugin repository.
And use [omnibus-td-agent](https://github.com/treasure-data/omnibus-td-agent) repository for td-agent releated issues.

Note: Before report the issue, check latest version first. Sometimes users report fixed bug with older version.

## Patch Guidelines

Here are some things that would increase a chance that your patch is accepted:

* Write tests.
* Run tests before send Pull Request by `bundle exec rake test`
* Write a [good commit message](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).
  * Fluentd repositories needs [DCO](https://github.com/apps/dco) on PR. Please add `Signed-off-by` to the commit(See DCO link for more detail).

There are some patches which are hard to write tests, e.g. process handling, concurrency issue or etc.
In such case, please don't hesitate to submit a Pull Request.
We can discuss how to manage a patch on Pull Request :)
