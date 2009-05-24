#!/usr/bin/env perl

use lib 'lib';
use strict;
use warnings;

#use Smart::Comments::JSON '##';
use Queue::Memcached::Buffered;
use Getopt::Std;
use JSON::XS;

my %opts;
getopts('hc:', \%opts) or help(1);
if ($opts{h}) {
    help(0);
}
my $count = $opts{c} || 0;

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

my $exported = 0;
while ($exported < $count and my $elem = $qmb->shift_elem) {
    print $json_xs->encode($elem), "\n";
    $exported++;
}
warn "For total $exported elements read from the queue \"$qname\" on $server\n";

sub help {
    my $status = shift;
    my $msg = <<'_EOC_';
USAGE:
    readqueue.pl -c <item-count> <queue-name>@<host>:<port> > <outfile>
_EOC_
    if ($status == 0) {
        print $msg;
        exit 0;
    }
    warn $msg;
    exit $status;
}

