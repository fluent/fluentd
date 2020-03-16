#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/plugin/output'
require 'fluent/plugin/out_forward/error'
require 'digest'

module Fluent::Plugin
  class ForwardOutput < Output
    class HandshakeProtocol
      def initialize(log:, hostname:, shared_key:, password:, username:)
        @log = log
        @hostname = hostname
        @shared_key = shared_key
        @password = password
        @username = username
        @shared_key_salt = generate_salt
      end

      def invoke(sock, ri, data)
        @log.trace __callee__

        case ri.state
        when :helo
          unless check_helo(ri, data)
            raise HeloError, 'received invalid helo message'
          end

          sock.write(generate_ping(ri).to_msgpack)
          ri.state = :pingpong
        when :pingpong
          succeeded, reason = check_pong(ri, data)
          unless succeeded
            raise PingpongError, reason
          end

          ri.state = :established
        else
          raise "BUG: unknown session state: #{ri.state}"
        end
      end

      private

      def check_pong(ri, message)
        @log.debug('checking pong')
        # ['PONG', bool(authentication result), 'reason if authentication failed',
        #  self_hostname, sha512\_hex(salt + self_hostname + nonce + sharedkey)]
        unless message.size == 5 && message[0] == 'PONG'
          return false, 'invalid format for PONG message'
        end
        _pong, auth_result, reason, hostname, shared_key_hexdigest = message

        unless auth_result
          return false, 'authentication failed: ' + reason
        end

        if hostname == @hostname
          return false, 'same hostname between input and output: invalid configuration'
        end

        clientside = Digest::SHA512.new.update(@shared_key_salt).update(hostname).update(ri.shared_key_nonce).update(@shared_key).hexdigest
        unless shared_key_hexdigest == clientside
          return false, 'shared key mismatch'
        end

        [true, nil]
      end

      def check_helo(ri, message)
        @log.debug('checking helo')
        # ['HELO', options(hash)]
        unless message.size == 2 && message[0] == 'HELO'
          return false
        end

        opts = message[1] || {}
        # make shared_key_check failed (instead of error) if protocol version mismatch exist
        ri.shared_key_nonce = opts['nonce'] || ''
        ri.auth = opts['auth'] || ''
        true
      end

      def generate_ping(ri)
        @log.debug('generating ping')
        # ['PING', self_hostname, sharedkey\_salt, sha512\_hex(sharedkey\_salt + self_hostname + nonce + shared_key),
        #  username || '', sha512\_hex(auth\_salt + username + password) || '']
        shared_key_hexdigest = Digest::SHA512.new.update(@shared_key_salt)
          .update(@hostname)
          .update(ri.shared_key_nonce)
          .update(@shared_key)
          .hexdigest
        ping = ['PING', @hostname, @shared_key_salt, shared_key_hexdigest]
        if !ri.auth.empty?
          if @username.nil? || @password.nil?
            raise PingpongError, "username and password are required"
          end

          password_hexdigest = Digest::SHA512.new.update(ri.auth).update(@username).update(@password).hexdigest
          ping.push(@username, password_hexdigest)
        else
          ping.push('', '')
        end
        ping
      end

      def generate_salt
        SecureRandom.hex(16)
      end
    end
  end
end
