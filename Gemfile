source 'https://rubygems.org/'

gemspec

gem 'chunkio', path: '/Users/yuta.iwama/src/github.com/ganmacs/chunkio'

# https://github.com/socketry/async-io/blob/v1.23.1/async-io.gemspec#L21
if Gem::Version.create(RUBY_VERSION) >= Gem::Version.create('2.3.0')
  gem 'async-http', '~> 0.42'
end

local_gemfile = File.join(File.dirname(__FILE__), "Gemfile.local")
if File.exist?(local_gemfile)
  puts "Loading Gemfile.local ..." if $DEBUG # `ruby -d` or `bundle -v`
  instance_eval File.read(local_gemfile)
end
