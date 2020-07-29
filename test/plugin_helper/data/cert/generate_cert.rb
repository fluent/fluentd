require 'fluent/plugin_helper/cert_option'
require 'fileutils'

module CertUtil
  extend Fluent::PluginHelper::CertOption
end

WITHOUT_CA_DIR = './without_ca'.freeze
WITH_CA_DIR = './with_ca'.freeze
WITH_CERT_CHAIN_DIR = './cert_chains'.freeze

CA_OPTION = {
  private_key_length: 2048,
  country: 'US',
  state: 'CA',
  locality: 'Mountain View',
  common_name: 'ca.testing.fluentd.org',
  expiration: 30 * 86400 * 12 * 100,
  digest: :sha256,
}

SERVER_OPTION = {
  private_key_length: 2048,
  country: 'US',
  state: 'CA',
  locality: 'Mountain View',
  common_name: 'server.testing.fluentd.org',
  expiration: 30 * 86400 * 12 * 100,
  digest: :sha256,
}

def write_cert_and_key(cert_path, cert, key_path, key, passphrase)
  File.open(cert_path, 'w') { |f| f.write(cert.to_pem) }

  # Write the secret key (raw or encrypted by AES256) in PEM format
  key_str = passphrase ? key.export(OpenSSL::Cipher.new('AES-256-CBC'), passphrase) : key.export
  File.open(key_path, 'w') { |f| f.write(key_str) }
  File.chmod(0o600, cert_path, key_path)
end

def create_server_pair_signed_by_self(cert_path, private_key_path, passphrase)
  cert, key, _ = CertUtil.cert_option_generate_server_pair_self_signed(SERVER_OPTION)
  write_cert_and_key(cert_path, cert, private_key_path, key, passphrase)
  cert
end

def create_ca_pair_signed_by_self(cert_path, private_key_path, passphrase)
  cert, key, _ = CertUtil.cert_option_generate_ca_pair_self_signed(CA_OPTION)
  write_cert_and_key(cert_path, cert, private_key_path, key, passphrase)
  cert
end

def create_server_pair_signed_by_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, passphrase)
  cert, key, _ = CertUtil.cert_option_generate_server_pair_by_ca(ca_cert_path, ca_key_path, ca_key_passphrase, SERVER_OPTION)
  write_cert_and_key(cert_path, cert, private_key_path, key, passphrase)
  cert
end

def create_without_ca
  FileUtils.mkdir_p(WITHOUT_CA_DIR)
  cert_path = File.join(WITHOUT_CA_DIR, 'cert.pem')
  cert_key_path = File.join(WITHOUT_CA_DIR, 'cert-key.pem')
  cert_pass_path = File.join(WITHOUT_CA_DIR, 'cert-pass.pem')
  cert_key_pass_path = File.join(WITHOUT_CA_DIR, 'cert-key-pass.pem')

  create_server_pair_signed_by_self(cert_path, cert_key_path, nil)
  create_server_pair_signed_by_self(cert_pass_path, cert_key_pass_path, 'apple') # with passphrase
end

def create_with_ca
  FileUtils.mkdir_p(WITH_CA_DIR)
  cert_path = File.join(WITH_CA_DIR, 'cert.pem')
  cert_key_path = File.join(WITH_CA_DIR, 'cert-key.pem')
  ca_cert_path = File.join(WITH_CA_DIR, 'ca-cert.pem')
  ca_key_path = File.join(WITH_CA_DIR, 'ca-cert-key.pem')
  create_ca_pair_signed_by_self(ca_cert_path, ca_key_path, nil)
  create_server_pair_signed_by_ca(ca_cert_path, ca_key_path, nil, cert_path, cert_key_path, nil)

  cert_pass_path = File.join(WITH_CA_DIR, 'cert-pass.pem')
  cert_key_pass_path = File.join(WITH_CA_DIR, 'cert-key-pass.pem')
  ca_cert_pass_path = File.join(WITH_CA_DIR, 'ca-cert-pass.pem')
  ca_key_pass_path = File.join(WITH_CA_DIR, 'ca-cert-key-pass.pem')
  create_ca_pair_signed_by_self(ca_cert_pass_path, ca_key_pass_path, 'orange') # with passphrase
  create_server_pair_signed_by_ca(ca_cert_pass_path, ca_key_pass_path, 'orange', cert_pass_path, cert_key_pass_path, 'apple')
end

def create_cert_pair_chained_with_root_ca(ca_cert_path, ca_key_path, ca_key_passphrase, cert_path, private_key_path, passphrase)
  root_cert, root_key, _ = CertUtil.cert_option_generate_ca_pair_self_signed(CA_OPTION)
  write_cert_and_key(ca_cert_path, root_cert, ca_key_path, root_key, ca_key_passphrase)

  intermediate_ca_options = CA_OPTION.dup
  intermediate_ca_options[:common_name] = 'ca2.testing.fluentd.org'
  chain_cert, chain_key = CertUtil.cert_option_generate_pair(intermediate_ca_options, root_cert.subject)
  chain_cert.add_extension(OpenSSL::X509::Extension.new('basicConstraints', OpenSSL::ASN1.Sequence([OpenSSL::ASN1::Boolean(true)])))
  chain_cert.sign(root_key, 'sha256')

  cert, server_key, _ = CertUtil.cert_option_generate_pair(SERVER_OPTION, chain_cert.subject)
  cert.add_extension OpenSSL::X509::Extension.new('basicConstraints', OpenSSL::ASN1.Sequence([OpenSSL::ASN1::Boolean(false)]))
  cert.sign(chain_key, 'sha256')

  # write chained cert
  File.open(cert_path, 'w') do |f|
    f.write(cert.to_pem)
    f.write(chain_cert.to_pem)
  end

  key_str = passphrase ? server_key.export(OpenSSL::Cipher.new("AES-256-CBC"), passphrase) : server_key.export
  File.open(private_key_path, "w") { |f| f.write(key_str) }
  File.chmod(0600, cert_path, private_key_path)
end

def create_cert_chain
  FileUtils.mkdir_p(WITH_CERT_CHAIN_DIR)
  ca_cert_path = File.join(WITH_CERT_CHAIN_DIR, 'ca-cert.pem')
  ca_key_path = File.join(WITH_CERT_CHAIN_DIR, 'ca-cert-key.pem')

  cert_path = File.join(WITH_CERT_CHAIN_DIR, 'cert.pem')
  private_key_path = File.join(WITH_CERT_CHAIN_DIR, 'cert-key.pem')

  create_server_pair_chained_with_root_ca(ca_cert_path, ca_key_path, nil, cert_path, private_key_path, nil)
end

create_without_ca
create_with_ca
create_cert_chain
