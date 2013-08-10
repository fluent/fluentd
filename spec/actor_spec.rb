require 'fluentd/actor'

describe Fluentd::Actor do
  describe Fluentd::Actors::TimerActor
  it '#after' do
    actor = described_class.new

    expect(actor.after(0){}).to_not be_nil
  end
end