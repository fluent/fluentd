require 'fluentd'

conf = <<EOF
<source>
  type heartbeat
  tag test.hb
  message {"test":"heartbeat"}

  <error>
    type buffered_stdout
  </error>
</source>

<filter **>
  type copy
  process_group remotex

  <match>
    process_group remote
    type stdout
  </match>
</filter>

<match **>
  type roundrobin
  <store>
    type buffered_stdout
  </store>
</match>

<match **>
  type buffered_stdout
</match>
EOF

Fluentd::Supervisor.run do
  Fluentd::Config.parse(conf, "(test)")
end

