source 'https://rubygems.org/'

gemspec

gem 'test-unit', github: 'nurse/test-unit', branch: 'code-snippet-fetcher-set_encoding'

local_gemfile = File.join(File.dirname(__FILE__), "Gemfile.local")
if File.exist?(local_gemfile)
  puts "Loading Gemfile.local ..." if $DEBUG # `ruby -d` or `bundle -v`
  instance_eval File.read(local_gemfile)
end
