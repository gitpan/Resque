package Resque::Stat;
{
  $Resque::Stat::VERSION = '0.03';
}
use Any::Moose;

# ABSTRACT: The stat subsystem. Used to keep track of integer counts.

has resque => (
    is       => 'ro',
    required => 1,
    handles  => [qw/ redis key /]
);

sub get {
    my ($self, $stat) = @_;
    $self->redis->get( $self->key( stat => $stat ) ) || 0;
}

sub incr {
    my ( $self, $stat, $by ) = @_;
    $by ||= 1;
    $self->redis->incrby( $self->key( stat => $stat ), $by );
}

sub decr {
    my ( $self, $stat, $by ) = @_;
    $by ||= 1;
    $self->redis->decrby( $self->key( stat => $stat ), $by );
}

sub clear {
    my ( $self, $stat ) = @_;
    $self->redis->del( $self->key( stat => $stat ) );
}

__PACKAGE__->meta->make_immutable();


__END__
=pod

=head1 NAME

Resque::Stat - The stat subsystem. Used to keep track of integer counts.

=head1 VERSION

version 0.03

=head1 ATTRIBUTES

=head2 resque

=head1 METHODS

=head2 get

Returns the int value of a stat, given a string stat name.

=head2 incr

For a string stat name, increments the stat by one.

Can optionally accept a second int parameter. The stat is then
incremented by that amount.

=head2 decr

For a string stat name, decrements the stat by one.

Can optionally accept a second int parameter. The stat is then
decremented by that amount.

=head2 clear

Removes a stat from Redis, effectively setting it to 0.

=head1 AUTHOR

Diego Kuperman <diego@freekeylabs.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Diego Kuperman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

