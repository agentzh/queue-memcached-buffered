#!/usr/bin/env perl

use lib 'lib';
use strict;
use warnings;

#use Smart::Comments::JSON '##';
use Queue::Memcached::Buffered;
use JSON::XS;

my $json_xs = JSON::XS->new->allow_nonref;

## @ARGV
my $queue = shift or
    die "No queue specified.\n";

my ($qname, $server);
if ($queue =~ /^\s*([^\@]+)\@([^:]+:\d+)\s*$/) {
    ($qname, $server) = ($1, $2);
} else {
    die "Invalid queue syntax: $queue\n",
        "\t(should be of the form queue-name\@host:port)\n";
}
my $qmb = Queue::Memcached::Buffered->new({
    queue => $qname,
    servers => [$server],
    item_size => 100,  # dummy
});

my $count = 0;
while (my $elem = $qmb->shift_elem) {
    print $json_xs->encode($elem), "\n";
    $count++;
}
warn "For total $count elements read from the queue \"$qname\" on $server\n";

