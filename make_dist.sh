#!/bin/sh
dst=fluent-0.9.0
rm -rf $dst
mkdir $dst || exit 1
cp -fpR lib bin $dst/ || exit 1
mkdir -p $dst/deps || exit 1
cp deps/*.gem deps/ruby-*.tar.bz2 $dst/deps/
cp README.md README COPYING NEWS ChangeLog AUTHORS INSTALL NOTICE \
    configure.in Makefile.in configure aclocal.m4 \
    Rakefile VERSION \
    $dst/ || exit 1
mkdir -p $dst/ac || exit 1
cp ac/* $dst/ac/ || exit 1
tar czvf $dst.tar.gz $dst || exit 1
rm -rf $dst

