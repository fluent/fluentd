FROM fluent/fluentd:v1.16-1

USER root

COPY fluent.conf /fluentd/etc/fluent.conf

# Install plugins if you want, e.g.:
# RUN gem install fluent-plugin-elasticsearch
RUN gem install fluent-plugin-syslog
ENV FLUENT_UID=0

USER fluent
