package Resque::Job;
{
  $Resque::Job::VERSION = '0.15';
}
use Moose;
use Moose::Util::TypeConstraints;
with 'Resque::Encoder';

# ABSTRACT: Resque job container

use overload '""' => \&stringify;
use Class::Load qw(load_class);

has resque  => ( 
    is      => 'rw', 
    handles => [qw/ redis /],
    default => sub { confess "This Resque::Job isn't associated to any Resque system yet!" } 
);

has worker  => ( 
    is      => 'rw', 
    lazy    => 1,
    default   => sub { $_[0]->resque->worker },
    predicate => 'has_worker'
);

has class   => ( is => 'rw', lazy => 1, default => sub { confess "This job needs a class to do some work." } );

has queue   => ( 
    is        => 'rw', lazy => 1, 
    default   => \&queue_from_class, 
    predicate => 'queued'
);

has args    => ( is => 'rw', isa => 'ArrayRef', default => sub {[]} );

coerce 'HashRef' 
    => from 'Str' 
    => via { JSON->new->utf8->decode($_) };
has payload => ( 
    is   => 'ro', 
    isa  => 'HashRef', 
    coerce => 1,
    lazy => 1,
    default => sub {{
        class => $_[0]->class,
        args  => $_[0]->args
    }},
    trigger => sub {
        my ( $self, $hr ) = @_;
        $self->class( $hr->{class} );
        $self->args( $hr->{args} ) if $hr->{args};
    }
);

sub encode {
    my $self = shift;
    $self->encoder->encode( $self->payload );
}

sub stringify {
    my $self = shift;
    sprintf( "(Job{%s} | %s | %s)", 
        $self->queue, 
        $self->class, 
        $self->encoder->encode( $self->args ) 
    );
}

sub queue_from_class {
    my $self = shift;
    my $class = $self->class;
    $class =~ s/://g;
    $class;
}

sub perform {
    my $self = shift;
    load_class($self->class);
    $self->class->can('perform') 
        || confess $self->class . " doesn't know how to perform";

    no strict 'refs';
    &{$self->class . '::perform'}($self); 
}

sub enqueue {
    my $self = shift;
    $self->resque->push( $self->queue, $self );
}

sub dequeue {
    my $self = shift;
    $self->resque->mass_dequeue({
        queue => $self->queue,
        class => $self->class,
        args  => $self->args
    });
}

sub fail {
    my ( $self, $error ) = @_;

    my $exception = 'Resque::Failure::Job';
    if ( ref $error && ref $error eq 'ARRAY' ) {
        ( $exception, $error ) = @$error; 
    }

    $self->resque->throw(
        job       => $self,
        worker    => $self->worker,
        queue     => $self->queue,
        payload   => $self->payload,
        exception => $exception,
        error     => $error
    );
}

__PACKAGE__->meta->make_immutable();


__END__
=pod

=head1 NAME

Resque::Job - Resque job container

=head1 VERSION

version 0.15

=head1 ATTRIBUTES

=head2 resque

=head2 worker

Worker running this job.
A new worker will be popped up from resque by default.

=head2 class

Class to be performed by this job.

=head2 queue

Name of the queue this job is or should be.

=head2 args

Array of arguments for this job.

=head2 payload

HashRef representation of the job.
When passed to constructor, this will restore the job from encoded state.
When passed as a string this will be coerced using JSON decoder.
This is read-only.

=head1 METHODS

=head2 encode

String representation(JSON) to be used on the backend.

=head2 stringify

=head2 queue_from_class

Normalize class name to be used as queue name.

    NOTE: future versions will try to get the
          queue name from the real class attr
          or $class::queue global variable.

=head2 perform

Load job class and call perform() on it.
This job objet will be passed as the only argument.

=head2 enqueue

Add this job to resque.
See Rescue::push().

=head2 dequeue

Remove this job from resque using the most restrictive
form of Resque::mass_dequeue.
This method will remove all jobs matching this 
object queue, class and args.

See Resque::mass_dequeue() for massive destruction. 

=head2 fail

Store a failure on this job.

=head1 AUTHOR

Diego Kuperman <diego@freekeylabs.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Diego Kuperman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

