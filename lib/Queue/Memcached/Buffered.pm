package Queue::Memcached::Buffered;

use strict;
use warnings;

our $VERSION = '0.01';

use JSON::XS;
use Memcached::libmemcached qw(
    memcached_create
    memcached_server_add
    memcached_set
    memcached_delete
);
use Encode qw( _utf8_on _utf8_off );

our $JsonXs = JSON::XS->new->utf8->allow_nonref;

sub new {
    my $class = ref $_[0] ? ref shift : shift;
    my $opts = shift || {};
    my $memc = delete $opts->{memc};
    my $servers = delete $opts->{servers};
    if (!defined $memc) {
        if ($servers) {
            if (!@$servers) {
                die "Empty servers given.\n";
            }
            $memc = memcached_create();
            #warn "memc: $memc";
            for my $server (@$servers) {
                my ($host, $port) = split /:/, $server;
                if (!defined $port) {
                    die "No port specified in server $server\n";
                }
                if ($port !~ /^\d+$/) {
                    die "Invalid port number \"$port\" in server $server\n";
                }
                memcached_server_add($memc, $host, $port);
            }
        } else {
            die "Neither memc nor servers specified.\n";
        }
    } elsif (defined $servers) {
        die "servers option not allowed when memc is already specified.\n";
    }
    my $queue = delete $opts->{queue} or
        die "No queue name specified.\n";
    my $item_size = delete $opts->{item_size} or
        die "No queue item size specified.\n";
    if (%$opts) {
        die "Unrecognized option names: ", join(' ', keys %$opts), "\n";
    }
    #warn "memc: $memc";
    return bless {
        memc      => $memc,
        queue     => $queue,
        item_size => $item_size,
        read_buf  => [],
        write_buf => '',
        write_buf_elem_count => 0,
    }, $class;
}

sub push_elem {
    my ($self, $elem) = @_;
    my $task_json = $JsonXs->encode($elem);
    my $queue = $self->{queue};
    my $flushed = 0;
    if (length($queue) + length($task_json) + length($self->{write_buf})
            > $self->{item_size}) { # clear the buffer
        if ($self->{write_buf} eq '') {
            die "single task too big: $task_json\n";
        }
        $flushed = $self->flush;
    }
    if ($self->{write_buf} eq '') {
        $self->{write_buf} = '[' . $self->{write_buf};
    } else {
        $self->{write_buf} .= ',' . $self->{write_buf};
    }
    $self->{write_buf_elem_count}++;
    return $flushed;
}

sub flush {
    my $self = shift;
    if ($self->{write_buf} eq '') {
        return 0;
    }
    my $queue = $self->{queue};
    my $memc = $self->{memc};
    $self->{write_buf} .= ']';
    ## len: length($tasks_json)
    ## $tasks_json
    memcached_set($memc, $queue, $self->{write_buf}) or
        die "line $.: Failed to add item to $queue: ", $memc->errstr, "\n";
    $self->{write_buf_elem_count} = 0;
    $self->{write_buf} = '';
    return 1;
}

sub write_buf_elem_count {
    $_[0]->{write_buf_elem_count};
}

sub shift_elem {
    my $self = shift;

}

sub DESTORY {
    my $self = shift;
    $self->flush;
}

1;
__END__

=head1 NAME

Queue::Memcached::Buffered - buffered queue API with read/write buffers

=head1 SYNOPSIS

    use Queue::Memcached::Buffered;

    my $queue = Queue::Memcached::Buffered->new({
        # or use memc => $memcached_object below:
        servers => ['127.0.0.1:11211', 'foo.bar.com:12345'],
        item_size => 1005,
        queue => 'queue_name',
    });
    for my $elem (@elems) {
        $queue->push_elem($elem);
    }
    $queue->flush; # don't forget this!

    while (my $elem = $queue->shift_elem) {
        # do something with $elem here...
    }

=head1 AUTHOR

Agent Zhang (agentzh) C<< <agentzh@yahoo.cn> >>

