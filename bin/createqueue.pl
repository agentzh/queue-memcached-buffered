#!/usr/bin/env perl

use lib 'lib';
use strict;
use warnings;
use Memcached::libmemcached qw(
    memcached_create
    memcached_server_add
    memcached_add
    memcached_free
);

my $queue = shift or
    die "No queue specified.\n";
my $size_limit = shift;
if (!defined $size_limit) {
    die "No size limit specified.\n";
}

my ($qname, $server);
if ($queue =~ /^\s*([^\@]+)\@([^:]+:\d+)\s*$/) {
    ($qname, $server) = ($1, $2);
} else {
    die "Invalid queue syntax: $queue\n",
        "\t(should be of the form queue-name\@host:port)\n";
}

my $memc = memcached_create();
my ($host, $port) = split /:/, $server;
if (!defined $port) {
    memcached_free($memc);
    die "No port specified in server $server\n";
}
if ($port !~ /^\d+$/) {
    memcached_free($memc);
    die "Invalid port number \"$port\" in server $server\n";
}
memcached_server_add($memc, $host, $port);
memcached_add($memc, $qname, $size_limit) or
    die "Failed to create queue $qname on server $server with size limit $size_limit: ", $memc->errstr, "\n";

