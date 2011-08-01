
SUBDIRS = deps/ruby

bin_SCRIPTS = fluent-gem fluent-cat fluentd

MOSTLYCLEANFILES = $(bin_SCRIPTS)

DEST_RUBY = $(DESTDIR)$(RUBY_BINDIR)/ruby
DEST_GEM = $(DEST_RUBY) -rrubygems -rrubygems/gem_runner -rrubygems/exceptions -rrubygems/installer -e 'class Gem::Installer; def shebang(bin_file_name) "\#\!$(RUBY_BINDIR)/ruby" end; end; Gem::GemRunner.new.run ARGV'
DEST_RAKE = $(DEST_RUBY) -rrake -e 'Rake.application.run'
DEST_RUBY_LIBDIR = $(DESTDIR)$(RUBY_LIBDIR)/ruby

fluent-gem:
	echo '#!/bin/sh' > $@
	echo 'exec "$(RUBY_BINDIR)/gem" "$$@"' >> $@
	chmod 755 $@

fluent-cat:
	echo '#!/bin/sh' > $@
	echo 'export FLUENT_CONF="$(sysconfdir)/fluent/fluent.conf"' >> $@
	echo 'export FLUENT_PLUGIN="$(sysconfdir)/fluent/plugin"' >> $@
	echo 'export FLUENT_SOCKET="$(localstatedir)/run/fluent.sock"' >> $@
	echo 'exec "$(RUBY_BINDIR)/ruby" "$(RUBY_BINDIR)/fluent-cat" "$$@"' >> $@
	chmod 755 $@

fluentd:
	echo '#!/bin/sh' > $@
	echo 'export FLUENT_CONF="$(sysconfdir)/fluent/fluent.conf"' >> $@
	echo 'export FLUENT_PLUGIN="$(sysconfdir)/fluent/plugin"' >> $@
	echo 'export FLUENT_SOCKET="$(localstatedir)/run/fluent.sock"' >> $@
	echo 'exec "$(RUBY_BINDIR)/ruby" "$(RUBY_BINDIR)/fluentd" "$$@"' >> $@
	chmod 755 $@

install: install-recursive
	for gem in $(srcdir)/deps/*.gem; do \
		RUBYLIB="$(DEST_RUBY_LIBDIR)/1.9.1:$(DEST_RUBY_LIBDIR)/1.9.1/$(shell $(DEST_RUBY) --version | sed -E 's/.*\[(.*)\]/\1/')" \
			$(DEST_GEM) install --no-rdoc --no-ri --env-shebang "$$gem"; \
	done
	cd "$(srcdir)" && \
		RUBYLIB="$(DEST_RUBY_LIBDIR)/1.9.1:$(DEST_RUBY_LIBDIR)/1.9.1/$(shell $(DEST_RUBY) --version | sed -E 's/.*\[(.*)\]/\1/')" \
		$(DEST_RAKE)
	RUBYLIB="$(DEST_RUBY_LIBDIR)/1.9.1:$(DEST_RUBY_LIBDIR)/1.9.1/$(shell $(DEST_RUBY) --version | sed -E 's/.*\[(.*)\]/\1/')" \
		$(DEST_GEM) install --no-rdoc --no-ri --env-shebang "$(srcdir)/pkg/fluent-$(VERSION).gem"
	mkdir -p $(DESTDIR)$(sysconfdir)/fluent
	cp -f $(srcdir)/fluent.conf $(DESTDIR)$(sysconfdir)/fluent/fluent.conf
	mkdir -p $(DESTDIR)$(sysconfdir)/fluent/plugin

dist:
	cd $(srcdir) && ./make_dist.sh

