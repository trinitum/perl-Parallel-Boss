use Test::Most;
use Parallel::Boss;

use Path::Tiny;
use POSIX ":sys_wait_h";
use Time::HiRes;
my $dir = Path::Tiny->tempdir;

my $worker = sub {
    my ( $boss, @args ) = @_;
    $dir->child("$$")->spew( map { "$_\n" } @args );
    while ( $boss->is_watching ) {
        sleep 5;
    }
};

my $pid = fork;
die "Couldn't fork: $!" unless defined $pid;

if ( $pid == 0 ) {
    alarm 120;
    Parallel::Boss->run(
        num_workers => 4,
        args        => [qw(foo bar baz)],
        worker      => $worker,
    );
    exit 0;
}

sub wait4files {
    my $tries = 20;
    my @files;
    while ($tries) {
        Time::HiRes::sleep(0.2);
        @files = $dir->children;
        last if @files == 4;
        --$tries or fail "Expected files were not created";
    }

    for (@files) {
        eq_or_diff [ $_->lines( { chomp => 1 } ) ], [qw(foo bar baz)],
          "expected content in $_";
    }

    return @files;
}

note "boss should start 4 workers";
my @files = wait4files();

note "if worker dies, boss should hire a new one";
$files[0]->remove;
kill KILL => $files[0]->basename;
@files = wait4files();

note "if boss receives SIGHUP it should kill workers and hire new ones";
$_->remove for @files;
kill HUP => $pid;
@files = wait4files();

note "if boss receives SIGTERM it should kill workers and quit";
my %pids = map { $_->basename => 1 } @files;
kill TERM => $pid;
my $tries = 20;
while ( $tries-- ) {
    Time::HiRes::sleep(0.2);
    my $kid = waitpid 0, WNOHANG;
    last if $kid == $pid;
}

fail "Boss is still running" if kill ZERO => $pid;
for ( keys %pids ) {
    fail "Worker $_ is still running" if kill ZERO => $_;
}

done_testing;
