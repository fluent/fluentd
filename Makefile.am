
SUBDIRS = deps/ruby

bin_SCRIPTS = fluent-gem fluent-cat fluentd

MOSTLYCLEANFILES = $(bin_SCRIPTS)

fluent-gem:
	echo '#!/bin/sh' > $@
	echo 'exec "$(RUBY_BINDIR)/gem" "$$@"' >> $@
	chmod 755 $@

fluent-cat:
	echo '#!/bin/sh' > $@
	echo 'export FLUENT_CONF="$(sysconfdir)/fluent/fluent.conf"' >> $@
	echo 'export FLUENT_PLUGIN="$(sysconfdir)/fluent/plugin"' >> $@
	echo 'export FLUENT_SOCKET="$(localstatedir)/run/fluent.sock"' >> $@
	echo 'exec "$(RUBY_BINDIR)/fluent-cat" "$$@"' >> $@
	chmod 755 $@

fluentd:
	echo '#!/bin/sh' > $@
	echo 'export FLUENT_CONF="$(sysconfdir)/fluent/fluent.conf"' >> $@
	echo 'export FLUENT_PLUGIN="$(sysconfdir)/fluent/plugin"' >> $@
	echo 'export FLUENT_SOCKET="$(localstatedir)/run/fluent.sock"' >> $@
	echo 'exec "$(RUBY_BINDIR)/fluentd" "$$@"' >> $@
	chmod 755 $@

install: install-recursive
	for gem in $(srcdir)/deps/*.gem; do \
		$(RUBY_BINDIR)/gem install --no-rdoc --no-ri "$$gem"; \
	done
	cd "$(srcdir)" && $(RUBY_BINDIR)/rake
	$(RUBY_BINDIR)/gem install --no-rdoc --no-ri "$(srcdir)/pkg/fluent-$(VERSION).gem"
	mkdir -p $(sysconfdir)/fluent
	cp -f $(srcdir)/fluent.conf $(sysconfdir)/fluent/fluent.conf
	mkdir -p $(sysconfdir)/plugin

dist:
	cd $(srcdir) && ./make_dist.sh

