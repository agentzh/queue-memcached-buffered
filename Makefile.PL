use strict;
use lib '.';
use inc::Module::Install;

name                ('Queue-Memcached-Buffered');
license             ('perl');
author              ('Agent Zhang <agentzh@yahoo.cn>');
perl_version        ('5.006001');
all_from            ('lib/Queue/Memcached/Buffered.pm');

requires            ('FindBin');
requires            ('Memcached::libmemcached' => '0.2101');

build_requires            ('Test::Base');
build_requires            ('Test::More');

#install_script('bin/prefetcher.pl');
#install_script('bin/cluster-prefetcher.pl');

no_index( directory => qw< t inc doc share demo > );

auto_install();
WriteAll();

