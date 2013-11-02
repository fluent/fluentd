require 'fluentd/event_router'
require 'fluentd/plugin/output'
require 'fluentd/plugin/output'
require 'fluentd/collectors/null_collector'
require 'fluentd/event_collection'

describe Fluentd::EventRouter do
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

  let! :default_collector do
    TestCollector.new
  end

  let! :error_handler do
    TestEmitErrorHandler.new
  end

  subject do
    Fluentd::EventRouter.new(default_collector, error_handler)
  end

  let :now do
    Time.now.to_i
  end

  let! :tag1_collector do
    c = TestCollector.new
    subject.add_pattern(Fluentd::MatchPattern.create("tag1.**"), c)
    c
  end

  let! :tag2_collector do
    c = TestCollector.new
    subject.add_pattern(Fluentd::MatchPattern.create("tag2.**"), c)
    c
  end

  describe '#emit' do
    it 'matches only one collector' do
      subject.emit("tag1", now, 'a'=>1)
      subject.emit("tag1", now, 'a'=>2)
      subject.emit("tag2", now, 'b'=>1)
      tag1_collector.events["tag1"].should == [Event(now, 'a'=>1), Event(now, 'a'=>2)]
      tag2_collector.events["tag2"].should == [Event(now, 'b'=>1)]
    end

    it 'routes logs to default collector if no collectors match' do
      subject.emit("no_such", now, 'c'=>1)
      default_collector.events["no_such"].should == [Event(now, 'c'=>1)]
    end

    it 'routes logs to error handler if output raises error' do
      subject.add_pattern(Fluentd::MatchPattern.create("error.**"), ErrorCollector.new)
      subject.emit("error.1", now, 'a'=>1)
      subject.emit("error.2", now, 'b'=>1)
      subject.emits("error.3", Fluentd::SingleEventCollection.new(now, 'c'=>1))
      error_handler.events["error.1"].should == [Event(now, 'a'=>1)]
      error_handler.events["error.2"].should == [Event(now, 'b'=>1)]
      error_handler.events["error.3"].should == [Event(now, 'c'=>1)]
    end

    it 'matches only one collector' do
      subject.emit("tag1", now, 'a'=>1)
      subject.emit("tag1", now, 'a'=>2)
      subject.emit("tag2", now, 'b'=>1)
      tag1_collector.events["tag1"].should == [Event(now, 'a'=>1), Event(now, 'a'=>2)]
      tag2_collector.events["tag2"].should == [Event(now, 'b'=>1)]
    end
  end

  describe '#emits' do
    let :es do
      Fluentd::SingleEventCollection.new(now, 'a'=>1)
    end

    it 'calls Controller#emits' do
      tag1_collector.should_receive(:emits).with("tag1", es)
      subject.emits("tag1", es)
    end

    it 'calls EmitErrorHandler#emits if output raises error' do
      subject.add_pattern(Fluentd::MatchPattern.create("error.**"), ErrorCollector.new)
      error_handler.should_receive(:handle_emits_error).with("error", es, kind_of(ErrorCollector::Error))
      subject.emits("error", es)
    end
  end

  describe '#match' do
    it 'caches matched tag' do
      subject.should_receive(:find).once.and_return(tag1_collector)
      tag1_collector.should_receive(:emit).exactly(10).times
      10.times { subject.emit("tag1", now, 'a'=>1) }
    end

    it 'caches unmatched tag' do
      subject.should_receive(:find).once.and_return(nil)
      default_collector.should_receive(:emit).exactly(10).times
      10.times { subject.emit("kumonantoka", now, 'a'=>1) }
    end

    it 'expires too many match cache' do
      subject.should_receive(:find).exactly(Fluentd::EventRouter::MATCH_CACHE_SIZE + 2).and_return(tag1_collector)
      10.times do
        Fluentd::EventRouter::MATCH_CACHE_SIZE.times do |i|
          subject.emit("tag1.#{i}", now, 'a'=>1)
        end
      end
      # calls :find and expires the oldest tag "tag1.0"
      subject.emit("tag1._", now, 'a'=>1)
      # calls :find and expires the oldest tag "tag1.1"
      subject.emit("tag1.0", now, 'a'=>1)
      # tag1.2 is still cached
      subject.emit("tag1.2", now, 'a'=>1)
    end
  end
end

