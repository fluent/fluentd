$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |gem|
  version_file = "lib/fluent/version.rb"
  version = File.read("VERSION").strip
  File.open(version_file, "w") {|f|
    f.write <<EOF
module Fluent

VERSION = '#{version}'

end
EOF
  }

  gem.name        = %q{fluentd}
  gem.version     = version
  # gem.platform  = Gem::Platform::RUBY
  gem.authors     = ["Sadayuki Furuhashi"]
  gem.email       = %q{frsyuki@gmail.com}
  gem.description = "Fluent event collector"
  gem.summary     = gem.description

  gem.homepage      = "http://fluentd.org/"
  gem.has_rdoc      = false
  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ['lib']

  gem.required_ruby_version = '~> 1.9.2'

  gem.add_dependency "msgpack", "~> 0.4.4"
  gem.add_dependency "json", ">= 1.4.3"
  gem.add_dependency "yajl-ruby", "~> 1.0.0"
  gem.add_dependency "cool.io", "~> 1.0.0"
  gem.add_dependency "http_parser.rb", "~> 0.5.1"
  gem.add_dependency "rake", ">= 0.9.2"
end