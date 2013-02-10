$:.push File.expand_path("../lib", __FILE__)
require 'fluentd/version'

Gem::Specification.new do |s|
  s.name = "fluentd"
  s.version = Fluentd::VERSION
  s.summary = "Fluentd lightweight and flexible log colector"
  s.description = "Fluentd receives logs as JSON streams, buffers them, and sends them to other systems like MySQL, MongoDB, or even other instances of Fluentd."
  s.author = "Sadayuki Furuhashi"
  s.email = "frsyuki@gmail.com"
  s.homepage = "http://fluentd.org/"
  s.require_paths = ["lib"]
  s.executables = ['fluentd']  #, 'fluent-cat', 'fluent-gem', 'fluent-debug']

  s.files = `git ls-files`.split("\n")
  s.test_files = `git ls-files -- {test,spec}/*`.split("\n")

  s.add_development_dependency 'bundler', ['>= 1.0.0']
  s.add_development_dependency 'rake', ['>= 0.8.7']
  s.add_development_dependency 'rspec', ['>= 2.10.0']
  s.add_development_dependency 'yard', ['~> 0.8']
  s.add_development_dependency 'simplecov', ['~> 0.6.4']
  s.add_dependency 'msgpack', ['~> 0.4.7']
  s.add_dependency "yajl-ruby", ["~> 1.0"]
  s.add_dependency "http_parser.rb", ["~> 0.5.1"]
  s.add_dependency 'cool.io', ['~> 1.1.0']
  s.add_dependency 'nio4r', ['~> 0.4.3']
  s.add_dependency 'timers', ['~> 1.1.0']
end
