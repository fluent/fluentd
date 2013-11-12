require 'fluentd/event_router'
require 'fluentd/plugin/output'
require 'fluentd/collectors/null_collector'
require 'fluentd/event_collection'

require_relative 'spec_classes'

describe Fluentd::EventRouter do
  let! :default_collector do
    TestCollector.new
  end

  let! :error_handler do
    TestEmitErrorHandler.new
  end

  let! :event_router do
    Fluentd::EventRouter.new(default_collector, error_handler)
  end

  let :now do
    Time.now.to_i
  end

  let! :tag1_collector do
    c = TestCollector.new
    event_router.add_rule(MatchPattern("tag1.**"), c)
    c
  end

  let! :tag2_collector do
    c = TestCollector.new
    event_router.add_rule(MatchPattern("tag2.**"), c)
    c
  end

  describe '#emit' do
    it 'matches only one collector' do
      event_router.emit("tag1", now, 'a'=>1)
      event_router.emit("tag1", now, 'a'=>2)
      event_router.emit("tag2", now, 'b'=>1)
      tag1_collector.events["tag1"].should == [Event(now, 'a'=>1), Event(now, 'a'=>2)]
      tag2_collector.events["tag2"].should == [Event(now, 'b'=>1)]
    end

    it 'routes logs to default collector if no collectors match' do
      event_router.emit("no_such", now, 'c'=>1)
      default_collector.events["no_such"].should == [Event(now, 'c'=>1)]
    end

    it 'routes logs to error handler if output raises error' do
      event_router.add_rule(MatchPattern("error.**"), ErrorCollector.new)
      event_router.emit("error.1", now, 'a'=>1)
      event_router.emit("error.2", now, 'b'=>1)
      event_router.emits("error.3", Fluentd::SingleEventCollection.new(now, 'c'=>1))
      error_handler.events["error.1"].should == [Event(now, 'a'=>1)]
      error_handler.events["error.2"].should == [Event(now, 'b'=>1)]
      error_handler.events["error.3"].should == [Event(now, 'c'=>1)]
    end

    it 'matches only one collector' do
      event_router.emit("tag1", now, 'a'=>1)
      event_router.emit("tag1", now, 'a'=>2)
      event_router.emit("tag2", now, 'b'=>1)
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
      event_router.emits("tag1", es)
    end

    it 'calls EmitErrorHandler#emits if output raises error' do
      event_router.add_rule(MatchPattern("error.**"), ErrorCollector.new)
      error_handler.should_receive(:handle_emits_error).with("error", es, kind_of(ErrorCollector::Error))
      event_router.emits("error", es)
    end
  end

  describe '#match' do
    it 'caches matched tag' do
      event_router.should_receive(:find).once.and_return(tag1_collector)
      tag1_collector.should_receive(:emit).exactly(10).times
      10.times { event_router.emit("tag1", now, 'a'=>1) }
    end

    it 'caches unmatched tag' do
      event_router.should_receive(:find).once.and_return(nil)
      default_collector.should_receive(:emit).exactly(10).times
      10.times { event_router.emit("kumonantoka", now, 'a'=>1) }
    end

    it 'expires too many match cache' do
      event_router.should_receive(:find).exactly(Fluentd::EventRouter::MATCH_CACHE_SIZE + 2).and_return(tag1_collector)
      10.times do
        Fluentd::EventRouter::MATCH_CACHE_SIZE.times do |i|
          event_router.emit("tag1.#{i}", now, 'a'=>1)
        end
      end
      # calls :find and expires the oldest tag "tag1.0"
      event_router.emit("tag1._", now, 'a'=>1)
      # calls :find and expires the oldest tag "tag1.1"
      event_router.emit("tag1.0", now, 'a'=>1)
      # tag1.2 is still cached
      event_router.emit("tag1.2", now, 'a'=>1)
    end
  end
end

