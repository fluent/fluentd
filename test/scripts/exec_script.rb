require 'json'
require 'msgpack'

def gen_tsv(time)
  "#{time}\ttag1\tok"
end

def gen_json(time)
  {'tag' => 'tag1', 'time' => time, 'k1' => 'ok'}.to_json
end

def gen_msgpack(time)
  {'tagger' => 'tag1', 'datetime' => time, 'k1' => 'ok'}.to_msgpack
end

def gen_raw_string(time)
  "#{time} hello"
end

time = ARGV.first
time = Integer(time) rescue time

case ARGV.last.to_i
when 0
  puts gen_tsv(time)
when 1
  puts gen_json(time)
when 2
  print gen_msgpack(time)
when 3
  print gen_raw_string(time)
end
