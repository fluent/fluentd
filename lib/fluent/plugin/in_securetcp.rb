require 'fluent/input'
require 'fluent/parser'
module Fluent
	class SecureTcpInput < Input
		Plugin.register_input('securetcp', self)
		
    def initialize
			super
			require "openssl"
			require "socket"
			require 'concurrent/executor/cached_thread_pool'
		end
		
    config_param :port, :integer, :default => 5433
		config_param :bind, :string, :default => '0.0.0.0'
		config_param :tag, :string, :default => nil
		config_param :format, :string, :default => nil
    config_param :add_ip, :bool, :default => nil
		config_param :max_connections, :integer, :default => nil # CachedThreadPool can't limit the number of threads
		config_param :ssl_certificate, :string, :default => nil
		config_param :ssl_key, :string, :default => nil
		config_param :ssl_key_passphrase, :string, :default => nil
		
    def configure(conf)		
    	super
			if !@tag && !@ssl_certificate && !@ssl_key
				      raise ConfigError,  "'tag' and 'ssl_certificate' and 'ssl_key'  parameter is required on secure tcp input"
			end
			if @format
				@parser = Plugin.new_parser(@format)
				@parser.configure(conf)
			end
			@connections = []
		end

		def start
			super
			tcp_client          = TCPServer.new(bind,port)
			ssl_context         = OpenSSL::SSL::SSLContext.new
			ssl_context.cert    = OpenSSL::X509::Certificate.new(File.read(@ssl_certificate))
      if @ssl_key_passphrase 
			  ssl_context.key     = OpenSSL::PKey::RSA.new(File.read(@ssl_key),@ssl_key_passphrase)
      else 
        ssl_context.key     = OpenSSL::PKey::RSA.new(File.read(@ssl_key))
      end
			@server             = OpenSSL::SSL::SSLServer.new(tcp_client, ssl_context)
			@thread_pool        = Concurrent::CachedThreadPool.new(:idletime => 50) 
			@thread             = Thread.new(&method(:run))
      log.info "SSL listening on port: #{bind}:#{port} *"
		end
		
    def shutdown
      @server.shutdown rescue nil
			@server.close rescue nil
		  @tcp_client.shutdown rescue nil
			@tcp_client.close rescue nil      
			@thread_pool.shutdown
			@thread.join
			super
		end
			
  	def run
			until @server.to_io.closed?			
      begin       
        conn = @server.accept
        next if conn.nil?
          if @max_connections
            @connections.reject! { |c| c.closed? }
            if @connections.size >= @max_connections
              conn.close # close for retry 
              sleep 1
              next
            end          
            @connections << conn
          end
          
          @thread_pool.post {
            begin
              while buf = conn.gets
                buf = buf.chomp
                to_server = buf 
                time = Time.now.getutc
                if @format
                  if @format = "json" && @add_ip 
                      sock_domain, remote_port, remote_hostname, remote_ip = conn.peeraddr
                      peer_ip = "\"peer_ip\":\"#{remote_ip}\","  
                      n = to_server.length
                      to_server = "{#{peer_ip}#{to_server.slice(1,n)}"
                   end
                  @parser.parse(to_server) { |time, record|
                    unless time && record 
                      log.warn "*pattern not match: #{msg.inspect} *"
                      next
                    end
                    
                    router.emit(tag,time,record)
                    }
                else
                  if @add_ip 
                    buff = conn.peeraddr.to_s + " - " + buff
                  end  
                  router.emit(tag,time,buf) 
                end
              end
            rescue OpenSSL::SSL::SSLError => e
              conn.close
              log.error "unexpected error", :error => e.to_s
              sleep 1
            rescue => e
                log.error "unexpected error:", :error => e.to_s
                next if e.message == "Connection reset by peer" #supress backtrace on connection reset
                log.error_backtrace
            end
          }
       rescue OpenSSL::SSL::SSLError => e
          conn.close
          log.error "unexpected error", :error => e.to_s
          sleep 1
       rescue => e
         if !@server.to_io.closed? #supress shutdown errors
            log.error "unexpected error", :error => e.to_s
            next if e.message == "Connection reset by peer" #supress backtrace on connection reset
            log.error_backtrace   
         end
       end   
     end
	end
end
end
