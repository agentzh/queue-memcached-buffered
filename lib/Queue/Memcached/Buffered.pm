package Queue::Memcached::Buffered;

use strict;
use warnings;

our $VERSION = '0.02';

use JSON::XS;
use Memcached::libmemcached qw(
    memcached_create
    memcached_server_add
    memcached_set
    memcached_get
    memcached_delete
);
use Encode qw( _utf8_on _utf8_off );

our $JsonXs = JSON::XS->new->utf8->allow_nonref;

sub new {
    my $class = ref $_[0] ? ref shift : shift;
    my $opts = shift || {};
    my $memc = delete $opts->{memc};
    my $servers = delete $opts->{servers};
    if (!defined $memc && !defined $servers) {
        die "Neither memc nor servers specified.\n";
    }
    my $queue = delete $opts->{queue} or
        die "No queue name specified.\n";
    my $item_size = delete $opts->{item_size} or
        die "No queue item size specified.\n";
    if (%$opts) {
        die "Unrecognized option names: ", join(' ', keys %$opts), "\n";
    }

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
                    memcached_free($memc);
                    die "No port specified in server $server\n";
                }
                if ($port !~ /^\d+$/) {
                    memcached_free($memc);
                    die "Invalid port number \"$port\" in server $server\n";
                }
                memcached_server_add($memc, $host, $port);
            }
        }
    } elsif (defined $servers) {
        die "servers option not allowed when memc is already specified.\n";
    }
    #warn "memc: $memc";
    return bless {
        memc      => $memc,
        servers   => $servers,
        queue     => $queue,
        item_size => $item_size,
        read_buf  => [],
        write_buf => '',
        write_buf_elem_count => 0,
    }, $class;
}

sub push_elem {
    my ($self, $elem) = @_;
    my $elem_json = $JsonXs->encode($elem);
    if (length($elem_json) + 2 > $self->{item_size}) {
        die "single task too big: $elem_json\n";
    }

    my $queue = $self->{queue};
    my $flushed = 0;
    if (length($queue) + length($elem_json) + length($self->{write_buf}) + 1
            > $self->{item_size}) { # clear the buffer
        $flushed = $self->flush;
        $self->{write_buf} = '[' . $elem_json;
        return $flushed;
    }
    if ($self->{write_buf} eq '') {
        $self->{write_buf} = '[' . $elem_json;
    } else {
        $self->{write_buf} .= ',' . $elem_json;
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
    ## len: length($tasks_json)
    ## $tasks_json
    memcached_set($memc, $queue, $self->{write_buf} . ']') or
        die "failed to add item to $queue: ", $memc->errstr, "\n";
    $self->{write_buf_elem_count} = 0;
    $self->{write_buf} = '';
    return 1;
}

sub write_buf_elem_count {
    $_[0]->{write_buf_elem_count};
}

sub shift_elem {
    my $self = shift;
    my $buf = $self->{read_buf};
    if (@$buf) {
        return shift @$buf;
    }
    my $queue = $self->{queue};
    my $memc = $self->{memc};
    my $elem_list_json = memcached_get($memc, $queue);
    if (!defined $elem_list_json) {
        if (defined $memc->errstr && $memc->errstr =~ /NOT FOUND/) {
            return undef;
        } else {
            die "failed to read item from $queue: ", $memc->errstr, "\n";
        }
    }

    my $elem_list;
    eval {
        $elem_list = $JsonXs->decode($elem_list_json);
    };
    if ($@) {
        die "failed to parse json $elem_list_json: $@\n";
    }
    if (!defined $elem_list || !ref $elem_list || ref $elem_list ne 'ARRAY') {
        die "invalid array returned from the server: $elem_list_json\n";
    }
    $self->{read_buf} = $elem_list;
    return shift @$elem_list;
}

sub size {
    my ($self) = @_;
    my $servers = $self->{servers};
    my $queue = $self->{queue};

    my ($size, $max_size);
    for my $server (@$servers) {
        next if !$server;

        my ($host, $port) = split /:/, $server;
        my $out = `(echo 'stats queue'; echo quit) | nc $host $port`;
        if ($out =~ /STAT \Q$queue\E (\d+) (\d+)/ms) {
            #warn $out;
            $size += $1;
            $max_size += $2;
        }
    }

    return wantarray ? ($size, $max_size) : $size;
}

sub DESTROY {
    my $self = shift;
    #warn "HERE!!!";
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

=head1 DIAGNOSTICS

Queue::Memcached::Buffered throw exceptions when error occurs. Here's the list of the common error messages:

=over

=item failed to add item to <queue>: <memcached errstr>

The C<push_elem> method fails to push the JSON item to the remote memcached queue server. The raw error messages would be given at the end.

=item failed to read item from <queue>: <memcached errstr>

The C<shift_elem> method fails to get a defined and non-empty JSON item from the remote memcached queue server. The raw error messages would be given at the end.

=item failed to parse JSON <json_string>: <errors from JSON::XS>

The server returns invalid JSON item and the C<shift_elem> method cannot parse it with L<JSON::XS>. The original error messages from L<JSON::XS> is given at the end.

=item invalid array returned from the server: <json string>

The memcached queue server returns a valid JSON string but it's not a JSON array as a whole, but JSON object, strings, numerals, or booleans.


=back

=head1 AUTHOR

Agent Zhang (agentzh) C<< <agentzh@yahoo.cn> >>

