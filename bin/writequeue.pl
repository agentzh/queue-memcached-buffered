#!/usr/bin/env perl

use lib 'lib';
use strict;
use warnings;

#use Smart::Comments::JSON '##';
use Queue::Memcached::Buffered;
use Getopt::Std;
use JSON::XS;

my %opts;
getopts('hs:', \%opts) or help(1);
if ($opts{h}) {
    help(0);
}
if (!$opts{s}) {
    die "No queue item sized specified by the option -s <num>.\n";
}

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
    item_size => $opts{s},  # dummy
});

my $count = 0;
while (<>) {
    my $json = $_;
    chomp;
    my $elem = $json_xs->decode($json);
    eval {
        $qmb->push_elem($elem);
    };
    if ($@) {
        die "Failed to push element at line $.: $@\n";
    }
    $count++;
}
warn "For total $count elements pushed to the queue \"$qname\" on $server\n";

sub help {
    my $status = shift;
    my $msg = <<'_EOC_';
USAGE:
    writequeue.pl -s <queue-item-size> <queue-name>@<host>:<port> <input-file>...
_EOC_
    if ($status == 0) {
        print $msg;
        exit 0;
    }
    warn $msg;
    exit $status;
}

