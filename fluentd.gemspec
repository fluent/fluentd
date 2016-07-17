require File.expand_path('../lib/fluent/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name          = "fluentd"
  gem.version       = Fluent::VERSION # see lib/fluent/version.rb

  gem.authors       = ["Sadayuki Furuhashi"]
  gem.email         = ["frsyuki@gmail.com"]
  gem.description   = %q{Fluentd is an open source data collector designed to scale and simplify log management. It can collect, process and ship many kinds of data in near real-time.}
  gem.summary       = %q{Fluentd event collector}
  gem.homepage      = "http://fluentd.org/"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.has_rdoc = false
  gem.license = "Apache-2.0"

  gem.required_ruby_version = '>= 2.1'

  gem.add_runtime_dependency("msgpack", [">= 0.7.0"])
  gem.add_runtime_dependency("json", [">= 1.4.3"])
  gem.add_runtime_dependency("yajl-ruby", ["~> 1.0"])
  gem.add_runtime_dependency("cool.io", [">= 1.4.3", "< 2.0.0"])
  gem.add_runtime_dependency("serverengine", [">= 1.6.4"])
  gem.add_runtime_dependency("http_parser.rb", [">= 0.5.1", "< 0.7.0"])
  gem.add_runtime_dependency("sigdump", ["~> 0.2.2"])
  gem.add_runtime_dependency("tzinfo", [">= 1.0.0"])
  gem.add_runtime_dependency("tzinfo-data", [">= 1.0.0"])
  if /mswin|mingw/ =~ RUBY_PLATFORM
    gem.add_runtime_dependency("win32-service", ["~> 0.8.3"])
    gem.add_runtime_dependency("win32-ipc", ["~> 0.6.1"])
    gem.add_runtime_dependency("win32-event", ["~> 0.6.1"])
    gem.add_runtime_dependency("windows-pr", ["~> 1.2.5"])
  end
  gem.add_runtime_dependency("strptime", [">= 0.1.7"])

  gem.add_development_dependency("rake", [">= 0.9.2"])
  gem.add_development_dependency("flexmock", ["~> 2.0.5"])
  gem.add_development_dependency("parallel_tests", [">= 0.15.3"])
  gem.add_development_dependency("simplecov", ["~> 0.6.4"])
  gem.add_development_dependency("rr", [">= 1.0.0"])
  gem.add_development_dependency("timecop", [">= 0.3.0"])
  gem.add_development_dependency("test-unit", ["~> 3.1.4"])
  gem.add_development_dependency("test-unit-rr", ["~> 1.0.3"])
  gem.add_development_dependency("oj", ["~> 2.14"])
end
