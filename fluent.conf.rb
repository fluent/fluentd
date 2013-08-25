
## built-in TCP input
## $ echo <json> | fluent-cat <tag>
source {
  type :forward
}

## built-in UNIX socket input
#source {
#  type :unix
#}

# HTTP input
# http://localhost:8888/<tag>?json=<json>
source {
  type :http
  port 8888
}

## File input
## read apache logs with tag=apache.access
#source {
#  type :tail
#  format :apache
#  path '/var/log/httpd-access.log'
#  tag 'apache.accesas'
#}

# Listen DRb for debug
source {
  type :debug_agent
  port 24230
}


## match tag=apache.access and write to file
#match('apache.access') {
#  type :file
#  path '/var/log/fluent/access'
#}

## match tag=debug.** and dump to console
match('debug.**') {
  type :stdout
}

## match tag=system.** and forward to another fluent server
#match('system.**') {
#  type :forward
#  host '192.168.0.11'
#  secondary {
#    host '192.168.0.12'
#  }
#}

# match tag=myapp.** and forward and write to file
match('myapp.**') {
  type :copy
  store {
    type :forward
    host '192.168.0.13'
    buffer_type :file
    buffer_path '/var/log/fluent/myapp-forward'
    retry_limit 50
    flush_interval '10s'
  }
  store {
    type :file
    path '/var/log/fluent/myapp'
  }
}

## match fluent's internal events
#match('fluent.**') {
#  type :null
#}

## match not matched logs and write to file
#match('**') {
#  type :file
#  path '/var/log/fluent/else'
#  compress :gz
#}

