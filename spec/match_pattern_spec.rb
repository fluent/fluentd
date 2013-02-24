require 'spec_helper'

include Fluentd

describe MatchPattern do
  describe MatchPattern do
    it { MatchPattern.create("a").should be_a_kind_of(GlobMatchPattern) }
    it { MatchPattern.create("a.**").should be_a_kind_of(GlobMatchPattern) }
    it { MatchPattern.create("").should be_a_kind_of(GlobMatchPattern) }
    it { MatchPattern.create("*").should be_a_kind_of(GlobMatchPattern) }
    it { MatchPattern.create("**").should be_a_kind_of(AllMatchPattern) }
  end

  describe AllMatchPattern do
    subject(:pattern) { AllMatchPattern.new }
    it { pattern.match?("a").should == true }
    it { pattern.match?(".").should == true }
    it { pattern.match?("").should == true }
  end

  RSpec::Matchers.define :glob_match do |tag|
    match do |pattern|
      m = GlobMatchPattern.new(pattern)
      m.match?(tag) == true
    end
  end

  describe GlobMatchPattern do
    context 'simple' do
      it { 'a'.should glob_match('a') }
      it { 'a.b'.should glob_match('a.b') }
      it { 'a'.should_not glob_match('b') }
      it { 'a.b'.should_not glob_match('aab') }
    end

    context 'wildcard' do
      it { 'a*'.should glob_match('a') }
      it { 'a*'.should glob_match('ab') }
      it { 'a*'.should glob_match('abc') }

      it { '*a'.should glob_match('a') }
      it { '*a'.should glob_match('ba') }
      it { '*a'.should glob_match('cba') }

      it { '*a*'.should glob_match('a') }
      it { '*a*'.should glob_match('ba') }
      it { '*a*'.should glob_match('ac') }
      it { '*a*'.should glob_match('bac') }

      it { 'a*'.should_not glob_match('a.b') }
      it { 'a*'.should_not glob_match('ab.c') }
      it { 'a*'.should_not glob_match('ba') }
      it { '*a'.should_not glob_match('ab') }

      it { 'a.*'.should glob_match('a.b') }
      it { 'a.*'.should glob_match('a.c') }
      it { 'a.*'.should_not glob_match('ab') }

      it { 'a.*.c'.should glob_match('a.b.c') }
      it { 'a.*.c'.should glob_match('a.c.c') }
      it { 'a.*.c'.should_not glob_match('a.c') }
    end

    context 'recursive wildcard' do
      it { 'a.**'.should glob_match('a') }
      it { 'a.**'.should_not glob_match('ab') }
      it { 'a.**'.should_not glob_match('abc') }
      it { 'a.**'.should glob_match('a.b') }
      it { 'a.**'.should_not glob_match('ab.c') }
      it { 'a.**'.should_not glob_match('ab.d.e') }

      it { 'a**'.should glob_match('a') }
      it { 'a**'.should glob_match('ab') }
      it { 'a**'.should glob_match('abc') }
      it { 'a**'.should glob_match('a.b') }
      it { 'a**'.should glob_match('ab.c') }
      it { 'a**'.should glob_match('ab.d.e') }

      it { '**.a'.should glob_match('a') }
      it { '**.a'.should_not glob_match('ba') }
      it { '**.a'.should_not glob_match('c.ba') }
      it { '**.a'.should glob_match('b.a') }
      it { '**.a'.should glob_match('cb.a') }
      it { '**.a'.should glob_match('d.e.a') }

      it { '**a'.should glob_match('a') }
      it { '**a'.should glob_match('ba') }
      it { '**a'.should glob_match('c.ba') }
      it { '**a'.should glob_match('b.a') }
      it { '**a'.should glob_match('cb.a') }
      it { '**a'.should glob_match('d.e.a') }
    end

    context 'test_or' do
      it { 'a.{b,c}'.should glob_match('a.b') }
      it { 'a.{b,c}'.should glob_match('a.c') }
      it { 'a.{b,c}'.should_not glob_match('a.d') }

      it { 'a.{b,c}.**'.should glob_match('a.b') }
      it { 'a.{b,c}.**'.should glob_match('a.c') }
      it { 'a.{b,c}.**'.should_not glob_match('a.d') }
      it { 'a.{b,c}.**'.should_not glob_match('a.cd') }

      it { 'a.{b.**,c}'.should glob_match('a.b') }
      it { 'a.{b.**,c}'.should glob_match('a.b.c') }
      it { 'a.{b.**,c}'.should glob_match('a.c') }
      it { 'a.{b.**,c}'.should_not glob_match('a.c.d') }
    end

    #context 'test_character_class' do
    #  it { '[a]'.should glob_match('a') }
    #  it { '[ab]'.should glob_match('a') }
    #  it { '[ab]'.should glob_match('b') }
    #  it { '[ab]'.should_not glob_match('c') }
    #
    #  it { '[a-b]'.should glob_match('a') }
    #  it { '[a-b]'.should glob_match('a') }
    #  it { '[a-b]'.should glob_match('b') }
    #  it { '[a-b]'.should_not glob_match('c') }
    #
    #  it { '[a-b0-9]'.should glob_match('a') }
    #  it { '[a-b0-9]'.should glob_match('0') }
    #  it { '[a-b0-9]'.should_not glob_match('c') }
    #end

  end
end

