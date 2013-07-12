require File.expand_path('../lib/fluent/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name          = "fluentd"
  gem.version       = Fluent::VERSION # see lib/fluent/version.rb

  gem.authors       = ["Sadayuki Furuhashi"]
  gem.email         = ["frsyuki@gmail.com"]
  gem.description   = %q{Fluentd is an event collector system. It is a generalized version of syslogd, which handles JSON objects for its log messages}
  gem.summary       = %q{Fluentd event collector}
  gem.homepage      = "http://fluentd.org/"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.has_rdoc = false

  gem.required_ruby_version = '>= 1.9.2'

  gem.add_runtime_dependency(%q<msgpack>, [">= 0.4.4", "!= 0.5.0", "!= 0.5.1", "!= 0.5.2", "!= 0.5.3", "< 0.6.0"])
  gem.add_runtime_dependency(%q<json>, [">= 1.4.3"])
  gem.add_runtime_dependency(%q<yajl-ruby>, ["~> 1.0"])
  gem.add_runtime_dependency(%q<cool.io>, ["~> 1.1.0"])
  gem.add_runtime_dependency(%q<http_parser.rb>, ["~> 0.5.1"])

  gem.add_development_dependency(%q<rake>, [">= 0.9.2"])
  gem.add_development_dependency(%q<rr>, [">= 1.0.0"])
  gem.add_development_dependency(%q<timecop>, [">= 0.3.0"])
end
