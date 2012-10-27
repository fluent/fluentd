require 'fluentd'

conf = <<EOF
<source>
  type heartbeat
  tag test.hb
  message {"test":"heartbeat"}
</source>

<filter **>
  type copy

  <match>
    process_group remote
    type stdout
  </match>
</filter>

<match **>
  type roundrobin
  <store>
    type stdout
  </store>
</match>

<match **>
  type buffered_stdout
</match>
EOF

Fluentd::Supervisor.run do
  Fluentd::Config.parse(conf, "(test)")
end

