use lib 'lib';
use strict;
use warnings;

#use Smart::Comments::JSON '##';
use Test::More tests => 46;
use Test::MockObject;

our @MockStack;

my $mock = Test::MockObject->new;

my $import;
my $memc_create_calls = 0;
my $memc_server_add_calls = 0;

$mock->fake_module(
    'Memcached::libmemcached',
    import => sub { $import = caller },
);

use_ok 'Queue::Memcached::Buffered';
is $import, 'Queue::Memcached::Buffered',
    'Queue::Memcached::Buffered should use Memcached::libmemcached';

my $queue;
eval {
    $queue = Queue::Memcached::Buffered->new;
};
is $@, "Neither memc nor servers specified.\n", 'exception thrown as expected';

eval {
    $queue = Queue::Memcached::Buffered->new({
        servers => ['127.0.0.1:11211', 'foo.bar.com:12345'],
    });
};
is $@, "No queue name specified.\n", 'queue name required';

eval {
    $queue = Queue::Memcached::Buffered->new({
        servers => ['127.0.0.1:11211', 'foo.bar.com:12345'],
        queue => 'blah',
    });
};
is $@, "No queue item size specified.\n", 'queue item size required';

eval {
    $queue = Queue::Memcached::Buffered->new({
        servers => ['127.0.0.1', 'foo.bar.com:12345'],
        queue => 'blah',
        item_size => 1005,
    });
};
is $@, "No port specified in server 127.0.0.1\n", 'no port given';

eval {
    $queue = Queue::Memcached::Buffered->new({
        servers => ['127.0.0.1:abc', 'foo.bar.com:12345'],
        queue => 'blah',
        item_size => 1005,
    });
};
is $@, "Invalid port number \"abc\" in server 127.0.0.1:abc\n", 'no port given';

@MockStack = ();

eval {
    $queue = Queue::Memcached::Buffered->new({
        servers => ['127.0.0.1:11211', 'foo.bar.com:12345'],
        queue => 'blah',
        item_size => 54,
    });
};
is $@, '', 'queue item size required';
ok $queue, 'queue defined';
isa_ok $queue, 'Queue::Memcached::Buffered';
is $queue->{queue}, 'blah', 'queue name initialized';
is $queue->{item_size}, 54, 'item_size initialized';
is $queue->{memc}, 'memc', 'memc inialized';

my $call = shift @MockStack;
is "@$call", 'memcached_create', 'memcached_create called as expected';

$call = shift @MockStack;
is "@$call", 'memcached_server_add memc 127.0.0.1 11211', 'memcached_server_add called (1)';

$call = shift @MockStack;
is "@$call", 'memcached_server_add memc foo.bar.com 12345', 'memcached_server_add called (2)';
#my %data = %$queue;
# %data
## @MockStack
is scalar(@MockStack), 0, 'no more call to memcached_*';

@MockStack = ();

my $flush = $queue->push_elem("hello world 1");
is $flush, 0, 'no flush';
is $queue->{write_buf}, '["hello world 1"', 'buf 1';

is scalar(@MockStack), 0, 'no memcached_* called when not flushed';
@MockStack = ();

$flush = $queue->push_elem("hello world 2");
is $flush, 0, 'no flush';
is $queue->{write_buf}, '["hello world 1","hello world 2"', 'buf 2';

is scalar(@MockStack), 0, 'no memcached_* called when not flushed';
@MockStack = ();

$flush = $queue->push_elem("hello world 3");
is $flush, 0, 'no flush';
my $expected_buf = '["hello world 1","hello world 2","hello world 3"';
is $queue->{write_buf}, $expected_buf, 'buf 3';
is scalar(@MockStack), 0, 'no memcached_* called when not flushed';
cmp_ok length($expected_buf) + 1, '<=', 53, 'less than 53';
#is length($expected_buf) + 1, 53, 'hello';
@MockStack = ();

$flush = $queue->push_elem("hello world 4");
is $flush, 1, 'no flush';
is $queue->{write_buf}, '["hello world 4"', 'buf 4';

is scalar(@MockStack), 1, 'no memcached_* called when not flushed';
$call = shift @MockStack;
is "@$call", "memcached_set memc blah $expected_buf]", 'memcached_set called as expected';
@MockStack = ();

$queue->flush;
is scalar(@MockStack), 1, 'memcached_set called...';
$call = shift @MockStack;
is "@$call", "memcached_set memc blah [\"hello world 4\"]", 'memcached_set called as expected';

@MockStack = ();

$queue = Queue::Memcached::Buffered->new({
    servers => ['127.0.0.1:11211', 'foo.bar.com:12345'],
    queue => 'blah',
    item_size => length('blah') + 49,
});

$flush = $queue->push_elem("hello world 1");
is $flush, 0, 'no flush';
is $queue->{write_buf}, '["hello world 1"', 'buf 1';

$flush = $queue->push_elem("hello world 2");
is $flush, 0, 'no flush';
is $queue->{write_buf}, '["hello world 1","hello world 2"', 'buf 2';

$flush = $queue->push_elem("hello world 3");
is $flush, 0, 'no flush';
$expected_buf = '["hello world 1","hello world 2","hello world 3"';
is $queue->{write_buf}, $expected_buf, 'buf 3';
is length($expected_buf . ']'), 49, 'exactly 49';

$flush = $queue->push_elem("hello world 4");
is $flush, 1, 'no flush';
is $queue->{write_buf}, '["hello world 4"', 'buf 4';

eval {
    $queue->push_elem("a" x 50);
};
is $@, qq{single task too big: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"\n}, '50 is just too big';

my $flushed = 0;
eval {
    $flushed = $queue->push_elem("a" x 49);
};
is $@, '', 'just okay';
is $flushed, 1;
is $queue->{write_buf}, '["'.('a'x49).'"', 'buf cleared';

#warn "$_ => $flush\n";

package Queue::Memcached::Buffered;

sub memcached_create {
    push @::MockStack, ['memcached_create', @_];
    #warn "HERE!";
    return 'memc';
}

sub memcached_server_add {
    push @::MockStack, ['memcached_server_add', @_];
    1;
}

sub memcached_free {
    push @::MockStack, ['memcached_free', @_];
    1;
}

sub memcached_set {
    push @::MockStack, ['memcached_set', @_];
}

