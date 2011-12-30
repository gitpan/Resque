package Resque::Failures;
{
  $Resque::Failures::VERSION = '0.04';
}
use Any::Moose;
with 'Resque::Encoder';

use UNIVERSAL::require;
use Carp;

has resque => ( 
    is       => 'ro', 
    required => 1, 
    handles  => [qw/ redis key /]
);

has failure_class => (
    is => 'rw', 
    lazy => 1, 
    default => sub { 
        'Resque::Failure::Redis'->require || confess $@;
        'Resque::Failure::Redis'; 
    },
    trigger => sub {
        my ( $self, $class ) = @_;
        $class->require or confess $@;
    }
);

sub throw {
    my $self = shift;
    my $e = $self->create(@_);
    $e->save;
}

sub create {
    my $self = shift;
    $self->failure_class->new( @_, resque => $self->resque );
}

sub count {
    my $self = shift;
    $self->redis->llen($self->key('failed'));
}

sub all {
    my ( $self, $start, $count ) = @_;
    my $all = $self->resque->list_range(
        $self->key('failed'), $start, $count
    );
    $_ = $self->encoder->decode( $_ ) for @$all;
    return wantarray ? @$all : $all;
}

sub clear {
    my $self = shift;
    $self->redis->del($self->key('failed'));
}

sub requeue {
    my ( $self, $index ) = @_;
    my $item = $self->all($index);
    $item->{retried_at} = DateTime->now->strftime("%Y/%m/%d %H:%M:%S");
    $self->redis->lset(
        $self->key('failed'), $index, 
        $self->encoder->encode($item)
    );
    $self->resque->push(
        $item->{queue} => { 
            class => $item->{payload}{class}, 
            args  => $item->{payload}{args}, 
    });
}

sub remove {
    my ( $self, $index ) = @_;
    my $id = rand(0xffffff);
    my $key = $self->key('failed');
    $self->redis->lset( $key, $index, $id);
    $self->redis->lrem( $key, 1, $id );
}

__PACKAGE__->meta->make_immutable();

__END__
=pod

=head1 NAME

Resque::Failures

=head1 VERSION

version 0.04

=head1 ATTRIBUTES

=head2 resque

Accessor to the Resque object.

=head2 failure_class

Name of a class consuming the role 'Resque::Failure'.
By default: Resque::Failure::Redis

=head1 METHODS

=head2 throw

create() a failure on the failure_class() and save() it.

=head2 create

Create a new failure on the failure_class() backend.

=head2 count

How many failures was in all the resque system.

=head2 all

Return a range of failures in the same way Resque::peek() does for
jobs.

=head2 clear

Remove all failures.

=head2 requeue

=head2 remove

=head1 AUTHOR

Diego Kuperman <diego@freekeylabs.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Diego Kuperman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

