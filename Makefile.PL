use 5.008;
use strict;
use warnings;
use ExtUtils::MakeMaker 6.64;

WriteMakefile(
    NAME                => 'Parallel::Boss',
    AUTHOR              => q{Pavel Shaydo <zwon@cpan.org>},
    LICENSE             => 'perl',
    VERSION_FROM        => 'lib/Parallel/Boss.pm',
    ABSTRACT_FROM       => 'lib/Parallel/Boss.pm',
    PL_FILES            => {},
    OBJECT              => "Boss.o",
    LIBS                => "-lpthread",
    PREREQ_PM => {
    },
    TEST_REQUIRES => {
        'Test::More' => 0.94,
    },
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 6.64,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Parallel-Boss-*' },
    META_MERGE => {
        resources => {
            license    => 'http://dev.perl.org/licenses/',
        },
        x_contributors => [
            'Pavel Shaydo <zwon@cpan.org>',
        ],
    },
);
