use lib 'lib';
use strict;
use warnings;

#use Smart::Comments::JSON '##';
use Test::More 'no_plan';
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
        servers => ['127.0.0.1:11211', 'foo.bar.com:12345'],
        queue => 'blah',
        item_size => 1005,
    });
};
is $@, '', 'queue item size required';
ok $queue, 'queue defined';
isa_ok $queue, 'Queue::Memcached::Buffered';
is $queue->{queue}, 'blah', 'queue name initialized';
is $queue->{item_size}, 1005, 'item_size initialized';
is $queue->{memc}, 'memc', 'memc inialized';

my $call = shift @MockStack; 
is "@$call", 'memcached_create', 'memcached_create called as expected';

$call = shift @MockStack;
is "@$call", 'memcached_server_add memc 127.0.0.1 11211', 'memcached_server_add called (1)';

$call = shift @MockStack;
is "@$call", 'memcached_server_add memc foo.bar.com 12345', 'memcached_server_add called (2)';
#my %data = %$queue;
# %data

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

