
# TODO move those classes to lib/fluentd/*

# TODO copied from plugin_spec_helper.rb
class TestCollector
  include Fluentd::Collector
  attr_accessor :events

  def initialize
    super
    @events = Hash.new {|hash,key| hash[key] = [] }
  end

  def emit(tag, time, record)
    @events[tag].push Event.new(time, record)
  end

  def emits(tag, es)
    es.each {|time,record| emit(tag, time, record) }
  end
end

class Event < Struct.new(:time, :record)
  alias_method :t, :time
  alias_method :r, :record
end

def Event(time, record)
  Event.new(time, record)
end

class TestEmitErrorHandler
  def initialize
    @events = Hash.new {|hash,key| hash[key] = [] }
  end

  attr_reader :events

  def handle_emit_error(tag, time, record, error)
    @events[tag].push Event.new(time, record)
  end

  def handle_emits_error(tag, es, error)
    es.each {|time,record| handle_emit_error(tag, time, record, error) }
  end
end

class ErrorCollector
  include Fluentd::Collector

  class Error < StandardError
  end

  def emit(tag, time, record)
    raise Error, "emit error"
  end

  def emits(tag, es)
    raise Error, "emits error"
  end
end

def MatchPattern(tag)
  Fluentd::MatchPattern.create(tag)
end

