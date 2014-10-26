package Resque::Failure::Redis;
{
  $Resque::Failure::Redis::VERSION = '0.16';
}
use Moose;
with 'Resque::Failure';
with 'Resque::Encoder';

# ABSTRACT: Redis backend for worker failures

sub save {
    my $self = shift;
    my $data = $self->encoder->encode({
        failed_at => $self->failed_at,
        payload   => $self->job->payload,
        exception => $self->exception,
        error     => $self->error,
        backtrace => $self->backtrace,
        worker    => $self->worker->id,
        queue     => $self->queue
    });
    $self->resque->redis->rpush( $self->resque->key( 'failed' ), $data );
}

__PACKAGE__->meta->make_immutable;

__END__
=pod

=head1 NAME

Resque::Failure::Redis - Redis backend for worker failures

=head1 VERSION

version 0.16

=head1 METHODS

=head2 save

Method required by L<Resque::Failure> role.

=head1 AUTHOR

Diego Kuperman <diego@freekeylabs.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Diego Kuperman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

