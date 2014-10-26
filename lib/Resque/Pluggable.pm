package Resque::Pluggable;
$Resque::Pluggable::VERSION = '0.18';
use Moose::Role;

# ABSTRACT: Role to load Resque plugin's and and apply roles.

use namespace::autoclean;
use Class::Load qw(load_class);
use Moose::Util qw(apply_all_roles);

has plugins => (
    is      => 'rw',
    isa     => 'ArrayRef',
    lazy    => 1,
    default => sub {[]}
);

has worker_class => (
    is   => 'ro',
    lazy => 1,
    default => sub { $_[0]->_class_with_roles('Resque::Worker' => 'worker') }
);

has job_class => (
    is   => 'ro',
    lazy => 1,
    default => sub { $_[0]->_class_with_roles('Resque::Job' => 'job') }
);

sub BUILD {} # Ensure it exists!
after BUILD => sub {
    my $self = shift;
    $self->_load_plugins;
    $self->meta->make_mutable;
    if ( my @r = $self->_roles_for('resque') ) {
        apply_all_roles( $self, @r );
    }
    $self->meta->make_immutable;
};

# Build anon class based on the given one with optional roles applied 
sub _class_with_roles {
    my ( $self, $class, $kind ) = @_;
    my @roles = $self->_roles_for($kind);
    load_class($class);
    return $class unless @roles;

    my $meta = Moose::Meta::Class->create_anon_class(
        superclasses => [$class],
        roles        => [@roles] 
    );

    # ensure anon doesn't goes out of scope!
    # http://www.nntp.perl.org/group/perl.moose/2008/03/msg132.html
    $meta->add_method( meta => sub {$meta} );

    $meta->make_immutable;
    return $meta->name;
}

sub _load_plugins {
    my $self = shift;

    my @plugins;
    for my $name ( @{$self->plugins} ) {
        $name = _expand_plugin($name);
        load_class($name);
        my $plugin = $name->new;
        push @plugins, $plugin;
    }
    $self->plugins(\@plugins);
}

sub _expand_plugin {
    my $name = pop;
    $name = $name =~ /^\+(.+)$/ ? $1 : "Resque::Plugin::$name";
    return $name;
}

sub _roles_for {
    my ( $self, $kind ) = @_;
    my $method_name = "${kind}_roles";

    my @roles;
    for my $plugin ( @{$self->plugins} ) {
        push @roles, map {$self->_expand_plugin($_)} @{$plugin->$method_name}
            if $plugin->can($method_name);
    }
    return @roles;
}

1;

__END__

=pod

=encoding UTF-8

=head1 NAME

Resque::Pluggable - Role to load Resque plugin's and and apply roles.

=head1 VERSION

version 0.18

=head1 ATTRIBUTES

=head2 plugins
List of plugins to be loaded into this L<Resque> system.

=head2 worker_class
Worker class to be used for worker attribute.
This is L<Resque::Worker> with all plugin/roles applied to it.

=head2 job_class
Job class to be used by L<Resque::new_job>.
This is L<Resque::Job> with all plugin/roles applied to it.

=head1 METHODS

=head2 BUILD
Apply pluggable roles after BUILD.

=head1 AUTHOR

Diego Kuperman <diego@freekeylabs.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Diego Kuperman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut
