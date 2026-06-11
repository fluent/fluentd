source 'https://rubygems.org/'

gemspec

gem 'benchmark'
gem 'json', git: 'https://github.com/byroot/json.git', branch: 'resumable-parser'

local_gemfile = File.join(File.dirname(__FILE__), "Gemfile.local")
if File.exist?(local_gemfile)
  puts "Loading Gemfile.local ..." if $DEBUG # `ruby -d` or `bundle -v`
  instance_eval File.read(local_gemfile)
end
