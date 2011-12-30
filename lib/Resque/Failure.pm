package Resque::Failure;
{
  $Resque::Failure::VERSION = '0.03';
}
use Any::Moose 'Role';
with 'Resque::Encoder';

# ABSTRACT: Role to be consumed by any failure class.

use overload '""' => \&stringify;
use DateTime;

requires 'save';

has 'worker' => ( 
    is       => 'ro',
    isa      => 'Resque::Worker', 
    required => 1
);

has 'job' => ( 
    is      => 'ro', 
    handles  => { 
        resque  => 'resque', 
        requeue => 'enqueue',
        payload => 'payload',
        queue   => 'queue',
    },
    required => 1
);

has created => ( 
    is      => 'rw',
    default => sub { DateTime->now } 
);

has failed_at => ( 
    is      => 'ro',
    lazy    => 1,
    default => sub { 
        $_[0]->created->strftime("%Y/%m/%d %H:%M:%S %Z"); 
    },
    predicate => 'has_failed_at'
);

has exception => ( 
    is      => 'rw',
    lazy    => 1,
    default => sub { 'Resque::Failure' }
);

has error     => ( is => 'rw', isa => 'Str', required => 1 );
has backtrace => ( is => 'rw', isa => 'Str' ); 

around error => sub {
    my $orig = shift;
    my $self = shift;

    return $self->$orig() unless @_;

    my ( $value, @stack ) = split "\n", shift;
    $self->backtrace( join "\n", @stack );
    return $self->$orig($value);
};

sub stringify { $_[0]->error }

1;

__END__
=pod

=head1 NAME

Resque::Failure - Role to be consumed by any failure class.

=head1 VERSION

version 0.03

=head1 METHODS

=head2 stringify

=head1 AUTHOR

Diego Kuperman <diego@freekeylabs.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Diego Kuperman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

