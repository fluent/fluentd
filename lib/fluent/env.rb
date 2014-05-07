module Fluent
  DEFAULT_CONFIG_PATH = ENV['FLUENT_CONF'] || '/etc/fluent/fluent.conf'
  DEFAULT_PLUGIN_DIR = ENV['FLUENT_PLUGIN'] || '/etc/fluent/plugin'
  DEFAULT_SOCKET_PATH = ENV['FLUENT_SOCKET'] || '/var/run/fluent/fluent.sock'
  DEFAULT_LISTEN_PORT = 24224
  DEFAULT_FILE_PERMISSION = 0644
  FLUENTD_OPTION_FOR_WINSVC = '-c C:/Ruby193/fluent/fluent.conf -o C:/Ruby193/fluent/fluentwinsvc.log'
end
