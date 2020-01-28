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

module Fluent
  module PluginHelper
    module HttpServer
      module Compat
        # This class converts OpenSSL::SSL::SSLContext to Webrick SSL Config because webrick does not have interface to pass OpenSSL::SSL::SSLContext directory
        # https://github.com/ruby/webrick/blob/v1.6.0/lib/webrick/ssl.rb#L67-L88
        class SSLContextExtractor

          #
          # memo: https://github.com/ruby/webrick/blob/v1.6.0/lib/webrick/ssl.rb#L180-L205
          # @param ctx [OpenSSL::SSL::SSLContext]
          def self.extract(ctx)
            {
              SSLEnable: true,
              SSLPrivateKey: ctx.key,
              SSLCertificate: ctx.cert,
              SSLClientCA: ctx.client_ca,
              SSLExtraChainCert: ctx.extra_chain_cert,
              SSLCACertificateFile: ctx.ca_file,
              SSLCACertificatePath: ctx.ca_path,
              SSLCertificateStore: ctx.cert_store,
              SSLTmpDhCallback: ctx.tmp_dh_callback,
              SSLVerifyClient: ctx.verify_mode,
              SSLVerifyDepth: ctx.verify_depth,
              SSLVerifyCallback: ctx.verify_callback,
              SSLServerNameCallback: ctx.servername_cb,
              SSLTimeout: ctx.timeout,
              SSLOptions: ctx.options,
              SSLCiphers: ctx.ciphers,
            }
          end
        end
      end
    end
  end
end
