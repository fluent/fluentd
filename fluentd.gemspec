require File.expand_path 'lib/fluentd/version', File.dirname(__FILE__)

Gem::Specification.new do |gem|
  gem.name = "fluentd"
  gem.version = Fluentd::VERSION
  gem.summary = "Fluentd lightweight and flexible log colector"
  gem.description = "Fluentd receives logs as JSON streams, buffers them, and sends them to other systems like MySQL, MongoDB, or even other instances of Fluentd."
  gem.author = "Sadayuki Furuhashi"
  gem.email = "frsyuki@gmail.com"
  gem.homepage = "http://fluentd.org/"

  gem.files = `git ls-files`.split($\)
  gem.executables = gem.files.grep(%r{^bin/}).map {|f| File.basename(f) }
  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency 'serverengine', ['~> 1.5.1']
  gem.add_dependency 'msgpack', ['~> 0.5.5']
  gem.add_dependency "yajl-ruby", ["~> 1.1"]
  gem.add_dependency "http_parser.rb", ["~> 0.5.1"]
  gem.add_dependency 'cool.io', ['~> 1.2.0']

  gem.add_development_dependency 'bundler', ['>= 1.0.0']
  gem.add_development_dependency "rake", [">= 0.9.2"]
  gem.add_development_dependency 'rspec', ['~> 2.13.0']
  gem.add_development_dependency 'yard', ['~> 0.8']
  gem.add_development_dependency 'simplecov', ['~> 0.6.4']
end
