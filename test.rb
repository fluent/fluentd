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
    type stdout
  </match>
</filter>

<match **>
  type stdout
</match>
EOF

Fluentd::Supervisor.run do
  Fluentd::Config.parse(conf, "(test)")
end

