package Resque::Worker;
{
  $Resque::Worker::VERSION = '0.10';
}
use Any::Moose;
with 'Resque::Encoder';

use Resque::Stat;
use POSIX ":sys_wait_h";
use Sys::Hostname;
use Scalar::Util qw(blessed weaken);
use List::MoreUtils qw(uniq any);
use Time::HiRes qw(sleep);
use DateTime;
use Try::Tiny;

# ABSTRACT: Does the hard work of babysitting Resque::Job's

use overload
    '""' => \&_string,
    '==' => \&_is_equal,
    'eq' => \&_is_equal;

has 'resque' => (
    is       => 'ro',
    required => 1,
    handles  => [qw/ redis key /]
);

has queues => (
    is      => 'rw',
    isa     => 'ArrayRef',
    lazy    => 1,
    default => sub {[]}
);

has stat => (
    is      => 'ro',
    lazy    => 1,
    default => sub { Resque::Stat->new( resque => $_[0]->resque ) }
);

has id => ( is => 'rw', lazy => 1, default => sub { $_[0]->_stringify } );
sub _string { $_[0]->id } # can't point overload to a mo[o|u]se attribute :-(

has verbose   => ( is => 'rw', default => sub {0} );

has cant_fork => ( is => 'rw', default => sub {0} );

has child    => ( is => 'rw' );

has shutdown => ( is => 'rw', default => sub{0} );

has paused   => ( is => 'rw', default => sub{0} );

has interval => ( is => 'rw', default => sub{5} );

sub pause           { $_[0]->paused(1) }

sub unpause         { $_[0]->paused(0) }

sub shutdown_please {
    print "Shutting down...\n";
    $_[0]->shutdown(1);
}

sub shutdown_now    { $_[0]->shutdown_please && $_[0]->kill_child }

sub work {
    my $self = shift;
    $self->startup;
    while ( ! $self->shutdown ) {
        if ( !$self->paused && ( my $job = $self->reserve ) ) {
            $self->log("Got job $job");
            $self->work_tick($job);
        }
        elsif( $self->interval ) {
            my $status = $self->paused ? "Paused" : 'Waiting for ' . join( ', ', @{$self->queues} );
            $self->procline( $status );
            $self->log( $status );
            sleep( $self->interval );
        }
    }
    $self->unregister_worker;
}

sub work_tick {
    my ($self, $job) = @_;

    $self->working_on($job);
    my $timestamp = DateTime->now->strftime("%Y/%m/%d %H:%M:%S %Z");

    if ( !$self->cant_fork && ( my $pid = fork ) ) {
        $self->procline( "Forked $pid at $timestamp" );
        $self->child($pid);
        $self->log( "Waiting for $pid" );
        #while ( ! waitpid( $pid, WNOHANG ) ) { } # non-blocking has sense?
        waitpid( $pid, 0 );
        $self->log( "Forked job($pid) exited with status $?" );
    }
    else {
        $self->procline( sprintf( "Processing %s since %s", $job->queue, $timestamp ) );
        $self->perform($job);
        exit(0) unless $self->cant_fork;
    }

    $self->done_working;
    $self->child(0);
}


sub perform {
    my ( $self, $job ) = @_;
    my $ret;
    try {
        $ret = $job->perform;
        $self->log( sprintf( "done: %s", $job->stringify ) );
    }
    catch {
        $self->log( sprintf( "%s failed: %s", $job->stringify, $_ ) );
        $job->fail($_);
        $self->failed(1);
    };
    $ret;
}

sub kill_child {
    my $self = shift;
    return unless $self->child;
    if ( kill 0, $self->child ) {
        $self->log( "Killing my child: " . $self->child );
        kill 9, $self->child;
    }
    else {
        $self->log( "Child " . $self->child . " not found, shutting down." );
        $self->shutdown_please;
    }
}

sub add_queue {
    my $self = shift;
    return unless @_;
    $self->queues( [ uniq( @{$self->queues}, @_ ) ] );
}

sub del_queue {
    my ( $self, $queue ) = @_;
    return unless $queue;

    return
    @{$self->queues}
           -
    @{$self->queues( [ grep {$_} map { $_ eq $queue ? undef : $_ } @{$self->queues} ] )};
}


sub next_queue {
    my $self = shift;
    if ( @{$self->queues} > 1 ) {
        push @{$self->queues}, shift @{$self->queues};
    }
    return $self->queues->[-1];
}

sub reserve {
    my $self = shift;
    my $count = 0;
    while ( my $queue = $self->next_queue ) {
        if ( my $job = $self->resque->pop($queue) ) {
            return $job;
        }
        return if ++$count == @{$self->queues};
    }
}

sub working_on {
    my ( $self, $job ) = @_;
    $self->redis->set(
        $self->key( worker => $self->id ),
        $self->encoder->encode({
            queue   => $job->queue,
            run_at  => DateTime->now->strftime("%Y/%m/%d %H:%M:%S %Z"),
            payload => $job->payload
        })
    );
    $job->worker($self);
}

sub done_working {
    my $self = shift;
    $self->processed(1);
    $self->redis->del( $self->key( worker => $self->id ) );
}

sub started {
    my $self = shift;
    $self->redis->get( $self->key( worker => $self->id => 'started' ) );
    #TODO -> parse datetime and return DT object.
}

sub set_started {
    my $self = shift;
    $self->redis->set( $self->key( worker => $self->id => 'started' ), DateTime->now->strftime('%Y-%m-%d %H:%M:%S %Z') );
}

sub processing {
    my $self = shift;
    eval { $self->encoder->decode( $self->redis->get( $self->key( worker => $self->id ) ) ) } || {};
}

sub state {
    my $self = shift;
    $self->redis->exists( $self->key( worker => $self->id ) ) ? 'working' : 'idle';
}

sub is_working {
    my $self = shift;
    $self->state eq 'working';
}

sub is_idle {
    my $self = shift;
    $self->state eq 'idle';
}

sub _stringify {
    my $self = shift;
    join ':', hostname, $$, join( ',', @{$self->queues} );
}

# Is this worker the same as another worker?
sub _is_equal {
    my ($self, $other) = @_;
    $self->id eq $other->id;
}

sub procline {
    my $self = shift;
    if ( my $str = shift ) {
        $0 = sprintf( "resque-%s: %s", $Resque::VERSION || 'devel', $str );
    }
    $0;
}

sub startup {
    my $self = shift;
    $0 = 'resque: Starting';

    $self->register_signal_handlers;
    $self->prune_dead_workers;
    #run_hook: before_first_fork
    $self->register_worker;
}

sub register_signal_handlers {
    my $self = shift;
    weaken $self;
    $SIG{TERM} = sub { $self->shutdown_now };
    $SIG{INT}  = sub { $self->shutdown_now };
    $SIG{QUIT} = sub { $self->shutdown_please };
    $SIG{USR1} = sub { $self->kill_child };
    $SIG{USR2} = sub { $self->pause };
    $SIG{CONT} = sub { $self->unpause };
}

sub prune_dead_workers {
    my $self = shift;
    my @all_workers   = $self->all;
    my @known_workers = $self->worker_pids if @all_workers;
    for my $worker (@all_workers) {
        my ($host, $pid, $queues) = split( ':', $worker->id );
        next unless $host eq hostname;
        next if any { $_ eq $pid } @known_workers;
        $self->log( "Pruning dead worker: $worker" );
        $worker->unregister_worker;
    }
}

sub register_worker {
    my $self = shift;
    $self->redis->sadd( $self->key( 'workers'), $self->id );
    $self->set_started;
}

sub unregister_worker {
    my $self = shift;

    # If we're still processing a job, make sure it gets logged as a
    # failure.
    {
        my $hr = $self->processing;
        if ( %$hr ) {
            # Ensure the proper worker is attached to this job, even if
            # it's not the precise instance that died.
            my $job = $self->resque->new_job({
                worker  => $self,
                queue   => $hr->{queue},
                payload => $hr->{payload}
            });
            $job->fail( 'Dirty exit' );
        }
    }

    $self->redis->srem( $self->key('workers'), $self->id );
    $self->redis->del( $self->key( worker => $self->id ) );
    $self->redis->del( $self->key( worker => $self->id => 'started' ) );

    $self->stat->clear("processed:$self");
    $self->stat->clear("failed:$self");
}

sub worker_pids {
    my $self = shift;
    my @pids;

    my $ps_command = $^O eq 'solaris'
                ? 'ps -A -o pid,args'
                : 'ps -A -o pid,command';

    for ( split "\n", `$ps_command | grep resque | grep -v resque-web` ) {
        if ( m/^\s*(\d+)\s(.+)$/ ) {
            push @pids, $1;
        }
    }
    return wantarray ? @pids : \@pids;
}

#TODO: add logger() attr to containg a logger object and if set, use that instead of print!
sub log {
    my $self = shift;
    return unless $self->verbose;
    print STDERR shift, "\n";
}

sub processed {
    my $self = shift;
    if (shift) {
        $self->stat->incr('processed');
        $self->stat->incr("processed:$self");
    }
    $self->stat->get("processed:$self");
}

sub failed {
    my $self = shift;
    if (shift) {
        $self->stat->incr('failed');
        $self->stat->incr("failed:$self");
    }
    $self->stat->get("failed:$self");
}

sub find {
    my ( $self, $worker_id ) = @_;
    if ( $self->exists( $worker_id ) ) {
        my @queues = split ',', (split( ':', $worker_id))[-1];
        return __PACKAGE__->new(
            resque => $self->resque,
            queues => \@queues,
            id     => $worker_id
        );
    }
}

sub all {
    my $self = shift;
    my @w = grep {$_} map { $self->find($_) } $self->redis->smembers( $self->key('workers') );
    return wantarray ? @w : \@w;
}

sub exists {
    my ($self, $worker_id) = @_;
    $self->redis->sismember( $self->key( 'workers' ), $worker_id );
}

__PACKAGE__->meta->make_immutable();


__END__
=pod

=head1 NAME

Resque::Worker - Does the hard work of babysitting Resque::Job's

=head1 VERSION

version 0.10

=head1 ATTRIBUTES

=head2 resque

The L<Resque> object running this worker.

=head2 queues

Queues this worker should fetch jobs from.

=head2 stat

See L<Resque::Stat>.

=head2 id

Unique identifier for the running worker.
Used to set process status all around.

The worker stringify to this attribute.

=head2 verbose

Set to a true value to make this worker report what's doing while
on work().

=head2 cant_fork

Set it to a true value to stop this worker from fork jobs.

By default, the worker will fork the job out and control the
children process. This make the worker more resilient to
memory leaks.

=head2 child

PID of current running child.

=head2 shutdown

When true, this worker will shutdown after finishing current job.

=head2 paused

When true, this worker won't proccess more jobs till false.

=head2 interval

Float representing the polling frequency. The default is 5 seconds, but for a semi-active app you may want to use a smaller value.

=head1 METHODS

=head2 pause

Stop processing jobs after the current one has completed (if we're
currently running one).

=head2 unpause

Start processing jobs again after a pause

=head2 shutdown_please

Schedule this worker for shutdown. Will finish processing the
current job.

=head2 shutdown_now

Kill the child and shutdown immediately.

=head2 work

Calling this method will make this worker to start pulling & running jobs
from queues().

This is the main wheel and will run while shutdown() is false.

=head2 work_tick

Perform() one job and wait till it finish.

=head2 perform

Call perform() on the given Resque::Job capturing and reporting
any exception.

=head2 kill_child

Kills the forked child immediately, without remorse. The job it
is processing will not be completed.

=head2 add_queue

Add a queue this worker should listen to.

=head2 del_queue

Stop listening to the given queue.

=head2 next_queue

Circular iterator over queues().

=head2 reserve

Pull the next job to be precessed.

=head2 working_on

Set worker and working status on the given L<Resque::Job>.

=head2 done_working

Inform the backend this worker has done its current job

=head2 started

What time did this worker start?
Returns an instance of DateTime.

TODO: not working in this release. This is returning
a string used internally.

=head2 set_started

Tell Redis we've started

=head2 processing

Returns a hash explaining the Job we're currently processing, if any.

=head2 state

Returns a string representing the current worker state,
which can be either working or idle

=head2 is_working

Boolean - true if working, false if not

=head2 is_idle

Boolean - true if idle, false if not

=head2 procline

Given a string, sets the procline ($0) and logs.
Procline is always in the format of:
    resque-VERSION: STRING

=head2 startup

Helper method called by work() to:

  1. register_signal_handlers()
  2. prune_dead_workers();
  3. register_worker();

=head2 register_signal_handlers

Registers the various signal handlers a worker responds to.

 TERM: Shutdown immediately, stop processing jobs.
  INT: Shutdown immediately, stop processing jobs.
 QUIT: Shutdown after the current job has finished processing.
 USR1: Kill the forked child immediately, continue processing jobs.
 USR2: Don't process any new jobs
 CONT: Start processing jobs again after a USR2

=head2 prune_dead_workers

Looks for any workers which should be running on this server
and, if they're not, removes them from Redis.

This is a form of garbage collection. If a server is killed by a
hard shutdown, power failure, or something else beyond our
control, the Resque workers will not die gracefully and therefore
will leave stale state information in Redis.

By checking the current Redis state against the actual
environment, we can determine if Redis is old and clean it up a bit.

=head2 register_worker

Registers ourself as a worker. Useful when entering the worker
lifecycle on startup.

=head2 unregister_worker

Unregisters ourself as a worker. Useful when shutting down.

=head2 worker_pids

Returns an Array of string pids of all the other workers on this
machine. Useful when pruning dead workers on startup.

=head2 log

If verbose() is true, this will print to STDERR.

=head2 processed

Retrieve from L<Resque::Stat> many jobs has done this worker.
Pass a true argument to increment by one.

=head2 failed

How many failed jobs has this worker seen.
Pass a true argument to increment by one.

=head2 find

Returns a single worker object. Accepts a string id.

=head2 all

Returns all worker registered on the backend.

=head2 exists

Returns true if the given worker id exists on redis() backend.

=head1 AUTHOR

Diego Kuperman <diego@freekeylabs.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Diego Kuperman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

