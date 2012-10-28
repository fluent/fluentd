module Fluentd
  DEFAULT_CONFIG_PATH = ENV['FLUENTD_CONF'] || '/etc/fluentd/fluentd.conf'
  DEFAULT_PLUGIN_DIR = ENV['FLUENTD_PLUGIN'] || '/etc/fluentd/plugin'
end
