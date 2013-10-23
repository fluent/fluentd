require 'fluentd/logger'

describe Fluentd::Logger do
  subject { Fluentd::Logger.new(STDERR) }

  # TODO more test

  it do
    subject.level = :trace
    subject.enable_filename = true

    subject.trace "test1"
    subject.trace { "test2" }
    subject.on_trace { subject.trace "test3" }
    begin
      raise "test"
    rescue
      subject.trace_backtrace
    end

  end
end
