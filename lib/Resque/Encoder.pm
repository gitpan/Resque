package Resque::Encoder;
{
  $Resque::Encoder::VERSION = '0.13';
}
use Moose::Role;
use JSON;

# ABSTRACT: Moose role for encoding Resque structures

has encoder => ( is => 'ro', default => sub { JSON->new->utf8 } );

1;

__END__
=pod

=head1 NAME

Resque::Encoder - Moose role for encoding Resque structures

=head1 VERSION

version 0.13

=head1 ATTRIBUTES

=head2 encoder

JSON encoder by default.

=head1 AUTHOR

Diego Kuperman <diego@freekeylabs.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Diego Kuperman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

