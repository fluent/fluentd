require "fluentd/match_pattern"

describe Fluentd::MatchPattern do

  def match(pat, str)
    m = Fluentd::GlobMatchPattern.new(pat)
    return m.match?(str)
  end

  context Fluentd::GlobMatchPattern do
    it "matches simple inputs" do
      expect(match('a', 'a')).to be_true
      expect(match('a.b', 'a.b')).to be_true
      expect(match('a', 'b')).to be_false
      expect(match('a.b', 'aab')).to be_false
    end

    it "matches wildcard" do
      expect(match('a*', 'a')).to be_true
      expect(match('a*', 'ab')).to be_true
      expect(match('a*', 'abc')).to be_true

      expect(match('*a', 'a')).to be_true
      expect(match('*a', 'ba')).to be_true
      expect(match('*a', 'cba')).to be_true

      expect(match('*a*', 'a')).to be_true
      expect(match('*a*', 'ba')).to be_true
      expect(match('*a*', 'ac')).to be_true
      expect(match('*a*', 'bac')).to be_true

      expect(match('a*', 'a.b')).to be_false
      expect(match('a*', 'ab.c')).to be_false
      expect(match('a*', 'ba')).to be_false
      expect(match('*a', 'ab')).to be_false

      expect(match('a.*', 'a.b')).to be_true
      expect(match('a.*', 'a.c')).to be_true
      expect(match('a.*', 'ab')).to be_false

      expect(match('a.*.c', 'a.b.c')).to be_true
      expect(match('a.*.c', 'a.c.c')).to be_true
      expect(match('a.*.c', 'a.c')).to be_false
    end


    it "matches recursive wildcard" do
      expect(match('a.**', 'a')).to be_true
      expect(match('a.**', 'ab')).to be_false
      expect(match('a.**', 'abc')).to be_false
      expect(match('a.**', 'a.b')).to be_true
      expect(match('a.**', 'ab.c')).to be_false
      expect(match('a.**', 'ab.d.e')).to be_false

      expect(match('a**', 'a')).to be_true
      expect(match('a**', 'ab')).to be_true
      expect(match('a**', 'abc')).to be_true
      expect(match('a**', 'a.b')).to be_true
      expect(match('a**', 'ab.c')).to be_true
      expect(match('a**', 'ab.d.e')).to be_true

      expect(match('**.a', 'a')).to be_true
      expect(match('**.a', 'ba')).to be_false
      expect(match('**.a', 'c.ba')).to be_false
      expect(match('**.a', 'b.a')).to be_true
      expect(match('**.a', 'cb.a')).to be_true
      expect(match('**.a', 'd.e.a')).to be_true

      expect(match('**a', 'a')).to be_true
      expect(match('**a', 'ba')).to be_true
      expect(match('**a', 'c.ba')).to be_true
      expect(match('**a', 'b.a')).to be_true
      expect(match('**a', 'cb.a')).to be_true
      expect(match('**a', 'd.e.a')).to be_true
    end

    it "matches 'or' condition" do
      expect(match('a.{b,c}', 'a.b')).to be_true
      expect(match('a.{b,c}', 'a.c')).to be_true
      expect(match('a.{b,c}', 'a.d')).to be_false

      expect(match('a.{b,c}.**', 'a.b')).to be_true
      expect(match('a.{b,c}.**', 'a.c')).to be_true
      expect(match('a.{b,c}.**', 'a.d')).to be_false
      expect(match('a.{b,c}.**', 'a.cd')).to be_false


      expect(match('a.{b.**,c}', 'a.b')).to be_true
      expect(match('a.{b.**,c}', 'a.b.c')).to be_true
      expect(match('a.{b.**,c}', 'a.c')).to be_true
      expect(match('a.{b.**,c}', 'a.c.d')).to be_false
    end
  end

end
