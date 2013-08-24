require 'fluentd/dns_resolver'
require 'ipaddr'

describe Fluentd::DNSResolver do
  context 'for IPv4 addresses' do
    describe '#resolve' do
      r = Fluentd::DNSResolver.new(:ipv4)

      it 'returns 127.0.0.1 for "localhost"' do
        expect(r.resolve('localhost')).to eql('127.0.0.1')
      end

      it 'returns IPv4 address for example.com' do
        expect(IPAddr.new(r.resolve('example.com')).ipv4?).to be_true
      end

      it 'returns IPv4 address for hostname of this host' do
        hostname = `hostname`.chop
        addr = begin
                 r.resolve(hostname)
               rescue Fluentd::ResolveError => e
                 nil # ignore
               end
        if addr
          expect(IPAddr.new().ipv4?).to be_true
        else
          expect(addr).to be_nil # IPv4 not ready environment
        end
      end

      it 'raises ResolveError for unknown host' do
        expect { r.resolve('non.existing.hostname.example.com') }.to raise_error(Fluentd::ResolveError)
      end
    end
  end

  context 'for IPv6 addresses' do
    describe '#resolve' do
      r = Fluentd::DNSResolver.new(:ipv6)

      it 'returns ::1 for "localhost"' do
        expect(r.resolve('localhost')).to eql('::1')
      end

      it 'returns IPv6 address for example.com' do
        expect(IPAddr.new(r.resolve('example.com')).ipv6?).to be_true
      end

      it 'returns IPv6 address for hostname of this host' do
        hostname = `hostname`.chop
        addr = begin
                 r.resolve(hostname)
               rescue Fluentd::ResolveError => e
                 nil # ignore
               end
        if addr
          expect(IPAddr.new(addr).ipv6?).to be_true
        else
          expect(addr).to be_nil # IPv6 not ready environment
        end
      end

      it 'raises ResolveError for unknown host' do
        expect { r.resolve('non.existing.hostname.example.com') }.to raise_error(Fluentd::ResolveError)
      end
    end
  end
end
