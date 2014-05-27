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

  gem.add_runtime_dependency("msgpack", [">= 0.4.4", "!= 0.5.0", "!= 0.5.1", "!= 0.5.2", "!= 0.5.3", "< 0.6.0"])
  gem.add_runtime_dependency("json", [">= 1.4.3"])
  gem.add_runtime_dependency("yajl-ruby", ["~> 1.0"])
  gem.add_runtime_dependency("cool.io", [">= 1.1.1", "!= 1.2.0", "< 2.0.0"])
  gem.add_runtime_dependency("http_parser.rb", [">= 0.5.1", "< 0.7.0"])
  gem.add_runtime_dependency("sigdump", ["~> 0.2.2"])
  gem.add_runtime_dependency("win32-service", ["~> 0.8.3"])


  gem.add_development_dependency("rake", [">= 0.9.2"])
  gem.add_development_dependency("flexmock")
  gem.add_development_dependency("parallel_tests", [">= 0.15.3"])
  gem.add_development_dependency("rspec", ["~> 2.13"])
  gem.add_development_dependency("simplecov", ["~> 0.6.4"])
  gem.add_development_dependency("rr", [">= 1.0.0"])
  gem.add_development_dependency("timecop", [">= 0.3.0"])
end
