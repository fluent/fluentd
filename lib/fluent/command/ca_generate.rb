require 'openssl'

module Fluent
  class CaGenerate
    def initialize(argv = ARGV)
      @argv = argv
    end

    def call
      ca_dir, passphrase = @argv

      unless ca_dir && passphrase
        puts 'USAGE: fluent-ca-generate  DIR_PATH  PRIVATE_KEY_PASSPHRASE'
        puts ''
        exit 0
      end

      FileUtils.mkdir_p(ca_dir)

      opt = {
        private_key_length: 2048,
        cert_country:  'US',
        cert_state:    'CA',
        cert_locality: 'Mountain View',
        cert_common_name: 'Fluentd Forward CA',
      }
      cert, key = Fluent::CaGenerate.generate_ca_pair(opt)

      key_data = key.export(OpenSSL::Cipher.new('aes256'), passphrase)
      File.open(File.join(ca_dir, 'ca_key.pem'), 'w') do |file|
        file.write key_data
      end
      File.open(File.join(ca_dir, 'ca_cert.pem'), 'w') do |file|
        file.write cert.to_pem
      end

      puts "successfully generated: ca_key.pem, ca_cert.pem"
      puts "copy and use ca_cert.pem to client(out_forward)"
    end

    def self.certificates_from_file(path)
      data = File.read(path)
      pattern = Regexp.compile('-+BEGIN CERTIFICATE-+\n(?:[^-]*\n)+-+END CERTIFICATE-+\n', Regexp::MULTILINE)
      list = []
      data.scan(pattern){|match| list << OpenSSL::X509::Certificate.new(match)}
      list
    end

    def self.generate_ca_pair(opts={})
      key = OpenSSL::PKey::RSA.generate(opts[:private_key_length])

      issuer = subject = OpenSSL::X509::Name.new
      subject.add_entry('C', opts[:cert_country])
      subject.add_entry('ST', opts[:cert_state])
      subject.add_entry('L', opts[:cert_locality])
      subject.add_entry('CN', opts[:cert_common_name])

      digest = OpenSSL::Digest::SHA256.new

      cert = OpenSSL::X509::Certificate.new
      cert.not_before = Time.at(0)
      cert.not_after = Time.now + 5 * 365 * 86400 # 5 years after
      cert.public_key = key
      cert.serial = 1
      cert.issuer = issuer
      cert.subject = subject
      cert.add_extension OpenSSL::X509::Extension.new('basicConstraints', OpenSSL::ASN1.Sequence([OpenSSL::ASN1::Boolean(true)]))
      cert.sign(key, digest)

      return cert, key
    end

    def self.generate_server_pair(opts={})
      key = OpenSSL::PKey::RSA.generate(opts[:private_key_length])

      ca_key = OpenSSL::PKey::RSA.new(File.read(opts[:ca_key_path]), opts[:ca_key_passphrase])
      ca_cert = OpenSSL::X509::Certificate.new(File.read(opts[:ca_cert_path]))
      issuer = ca_cert.issuer

      subject = OpenSSL::X509::Name.new
      subject.add_entry('C', opts[:country])
      subject.add_entry('ST', opts[:state])
      subject.add_entry('L', opts[:locality])
      subject.add_entry('CN', opts[:common_name])

      digest = OpenSSL::Digest::SHA256.new

      cert = OpenSSL::X509::Certificate.new
      cert.not_before = Time.at(0)
      cert.not_after = Time.now + 5 * 365 * 86400 # 5 years after
      cert.public_key = key
      cert.serial = 2
      cert.issuer = issuer
      cert.subject = subject

      cert.add_extension OpenSSL::X509::Extension.new('basicConstraints', OpenSSL::ASN1.Sequence([OpenSSL::ASN1::Boolean(false)]))
      cert.add_extension OpenSSL::X509::Extension.new('nsCertType', 'server')

      cert.sign ca_key, digest

      return cert, key
    end

    def self.generate_self_signed_server_pair(opts={})
      key = OpenSSL::PKey::RSA.generate(opts[:private_key_length])

      issuer = subject = OpenSSL::X509::Name.new
      subject.add_entry('C', opts[:country])
      subject.add_entry('ST', opts[:state])
      subject.add_entry('L', opts[:locality])
      subject.add_entry('CN', opts[:common_name])

      digest = OpenSSL::Digest::SHA256.new

      cert = OpenSSL::X509::Certificate.new
      cert.not_before = Time.at(0)
      cert.not_after = Time.now + 5 * 365 * 86400 # 5 years after
      cert.public_key = key
      cert.serial = 1
      cert.issuer = issuer
      cert.subject  = subject
      cert.sign(key, digest)

      return cert, key
    end
  end
end
