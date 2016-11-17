package Parallel::Boss;
use strict;
use warnings;
our $VERSION = "0.01";
$VERSION = eval $VERSION;

use IO::Select;

=head1 NAME

Parallel::Boss - manage worker processes

=head1 VERSION

This document describes Parallel::Boss version 0.01

=head1 SYNOPSIS

    use Parallel::Boss;

    my $worker = sub {
        my ( $boss, @args ) = @_;
        while ( $boss->is_watching ) {

            # pretend to be working
            ...;
        }
    };

    Parallel::Boss->run(
        num_workers => 4,
        args        => \@args,
        worker      => $worker,
    );

=head1 DESCRIPTION

Module running specified number of worker processes.

=head1 METHODS

=cut

=head2 run

     $class->run(%params)

start specified number of workers and supervise them. If any of the workers
exits, a new one will be started as a replacement. If parent process receives
HUP signal, then it sends HUP signal to every worker process and restarts
workers if they exit. If parent process receives INT, QUIT, or TERM, it sends
TERM to all workers, waits for up to 15 seconds till they all exit, and sends
KILL to those workers that are still running, after all workers exited the run
method returns.

The following parameters are accepted:

=over 4

=item B<num_workers>

number of workers to start

=item B<args>

reference to array of arguments that should be passed to worker subroutine

=item B<worker>

subroutine that will be executed by every worker. If it returns, the worker
process exits. The subroutine passed the Parallel::Boss object as the first
argument, and array specified by I<args> as the following arguments.

=back

=cut

sub run {
    my ( $class, %args ) = @_;

    my $self = bless \%args, $class;

    pipe( my $rd, my $wr ) or die "Couldn't create a pipe";
    $self->{_rd} = $rd;
    $self->{_wr} = $wr;

    local $SIG{QUIT} = local $SIG{INT} = local $SIG{TERM} = sub {
        $self->_kill_children("TERM");
        $self->{_finish} = 1;
        $self->{_wr}->close;
        alarm 15;
    };
    local $SIG{HUP} = sub { $self->_kill_children("HUP"); };
    local $SIG{ALRM} = sub {
        $self->_kill_children("KILL") if $self->{_finish};
    };

    for ( 1 .. $self->{num_workers} ) {
        $self->_spawn;
    }

    while (1) {
        my $pid = wait;
        delete $self->{_workers}{$pid} or next;
        last if $self->{_finish} and not keys %{ $self->{_workers} };
        $self->_spawn unless $self->{_finish};
    }
}

sub _spawn {
    my ($self) = @_;
    my $pid = fork;
    if ( not defined $pid ) {
        $self->_kill_children("KILL");
        die "Couldn't fork, exiting: $!";
    }

    if ($pid) {
        $self->{_workers}{$pid} = 1;
    }
    else {
        $self->{_wr}->close;
        $SIG{$_} = 'DEFAULT' for qw(QUIT HUP INT TERM ALRM);
        $self->{worker}->( $self, @{ $self->{args} } );
        exit 0;
    }
}

sub _kill_children {
    my ($self, $sig) = @_;

    kill $sig => keys %{ $self->{_workers} };
}

=head2 is_watching

     $boss->is_watching

this method should be periodically invoked by the worker process. It checks if
the parent process is still running, if not the worker should exit.

=cut

sub is_watching {
    my ($self) = @_;
    $self->{_select} //= IO::Select->new( $self->{_rd} );
    return if $self->{_select}->can_read;
    return 1;
}

1;

__END__

=head1 AUTHOR

Pavel Shaydo C<< <zwon at cpan.org> >>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2016 Pavel Shaydo

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
