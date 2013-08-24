require 'socket'
require 'ipaddr'
require 'resolv'

module Fluentd
  class ResolveError < StandardError
  end

  class DNSResolver
    def initialize(proto=:ipv4)
      @proto = proto
      @resolver = Resolv::DNS.new
    end

    def ipv4? ; @proto == :ipv4 ; end
    def ipv6? ; @proto == :ipv6 ; end

    def resolve(name)
      if name.downcase == 'localhost'
        return ipv4? ? '127.0.0.1' : '::1'
      end

      begin
        ipaddr = IPSocket.getaddress(name)
        return ipaddr if ipv4? && IPAddr.new(ipaddr).ipv4? || ipv6? && IPAddr.new(ipaddr).ipv6?
      rescue SocketError => e
        # ignore
      end

      t = ipv6? ? Resolv::DNS::Resource::IN::AAAA : Resolv::DNS::Resource::IN::A
      begin
        return Resolv::DNS.new.getresource(name, t).address.to_s
      rescue Resolv::ResolvError => e
        raise unless e.message.start_with?('DNS result has no information for')
      end

      raise ResolveError, "Failed to resolve host name #{name}"
    end
  end
end
