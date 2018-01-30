require 'openssl'
require 'optparse'
require 'fileutils'
require 'fluent/version'

module Fluent
  class CaGenerate
    DEFAULT_OPTIONS = {
      private_key_length: 2048,
      cert_country:  'US',
      cert_state:    'CA',
      cert_locality: 'Mountain View',
      cert_common_name: 'Fluentd Forward CA',
    }
    HELP_TEXT = <<HELP
Usage: fluent-ca-generate DIR_PATH PRIVATE_KEY_PASSPHRASE [--country COUNTRY] [--state STATE] [--locality LOCALITY] [--common-name COMMON_NAME]
HELP

    def initialize(argv = ARGV)
      @argv = argv
      @options = {}
      @opt_parser = OptionParser.new
      configure_option_parser
      @options.merge!(DEFAULT_OPTIONS)
      parse_options!
    end

    def usage(msg = nil)
      puts HELP_TEXT
      puts "Error: #{msg}" if msg
      exit 1
    end

    def call
      ca_dir, passphrase, = @argv[0..1]

      unless ca_dir && passphrase
        puts "#{HELP_TEXT}"
        puts ''
        exit 1
      end

      FileUtils.mkdir_p(ca_dir)

      cert, key = Fluent::CaGenerate.generate_ca_pair(@options)

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

    private

    def configure_option_parser
      @opt_parser.banner = HELP_TEXT
      @opt_parser.version = Fluent::VERSION

      @opt_parser.on('--key-length [KEY_LENGTH]',
                     "configure key length. (default: #{DEFAULT_OPTIONS[:private_key_length]})") do |v|
        @options[:private_key_length] = v.to_i
      end

      @opt_parser.on('--country [COUNTRY]',
                     "configure country. (default: #{DEFAULT_OPTIONS[:cert_country]})") do |v|
        @options[:cert_country] = v.upcase
      end

      @opt_parser.on('--state [STATE]',
                     "configure state. (default: #{DEFAULT_OPTIONS[:cert_state]})") do |v|
        @options[:cert_state] = v
      end

      @opt_parser.on('--locality [LOCALITY]',
                     "configure locality. (default: #{DEFAULT_OPTIONS[:cert_locality]})") do |v|
        @options[:cert_locality] = v
      end

      @opt_parser.on('--common-name [COMMON_NAME]',
                     "configure common name (default: #{DEFAULT_OPTIONS[:cert_common_name]})") do |v|
        @options[:cert_common_name] = v
      end
    end

    def parse_options!
      @opt_parser.parse!(@argv)
    end
  end
end
