
use strict;
use warnings;



use ExtUtils::MakeMaker 6.30;



my %WriteMakefileArgs = (
  "ABSTRACT" => "Redis-backed library for creating background jobs, placing them on multiple queues, and processing them later.",
  "AUTHOR" => "Diego Kuperman <diego\@freekeylabs.com>",
  "BUILD_REQUIRES" => {},
  "CONFIGURE_REQUIRES" => {
    "ExtUtils::MakeMaker" => "6.30"
  },
  "DISTNAME" => "Resque",
  "EXE_FILES" => [],
  "LICENSE" => "perl",
  "NAME" => "Resque",
  "PREREQ_PM" => {
    "Carp" => 0,
    "Class::Load" => 0,
    "DateTime" => 0,
    "FindBin" => 0,
    "JSON" => 0,
    "List::MoreUtils" => 0,
    "Moose" => 0,
    "Moose::Exporter" => 0,
    "Moose::Role" => 0,
    "Moose::Util" => 0,
    "Moose::Util::TypeConstraints" => 0,
    "POSIX" => 0,
    "Redis" => 0,
    "Scalar::Util" => 0,
    "Sys::Hostname" => 0,
    "Time::HiRes" => 0,
    "Try::Tiny" => 0,
    "namespace::autoclean" => 0,
    "overload" => 0,
    "strict" => 0
  },
  "TEST_REQUIRES" => {
    "Exporter" => 0,
    "File::Temp" => 0,
    "IPC::Cmd" => 0,
    "Test::More" => "0.96",
    "base" => 0,
    "warnings" => 0
  },
  "VERSION" => "0.14",
  "test" => {
    "TESTS" => "t/*.t"
  }
);


unless ( eval { ExtUtils::MakeMaker->VERSION(6.63_03) } ) {
  my $tr = delete $WriteMakefileArgs{TEST_REQUIRES};
  my $br = $WriteMakefileArgs{BUILD_REQUIRES};
  for my $mod ( keys %$tr ) {
    if ( exists $br->{$mod} ) {
      $br->{$mod} = $tr->{$mod} if $tr->{$mod} > $br->{$mod};
    }
    else {
      $br->{$mod} = $tr->{$mod};
    }
  }
}

unless ( eval { ExtUtils::MakeMaker->VERSION(6.56) } ) {
  my $br = delete $WriteMakefileArgs{BUILD_REQUIRES};
  my $pp = $WriteMakefileArgs{PREREQ_PM};
  for my $mod ( keys %$br ) {
    if ( exists $pp->{$mod} ) {
      $pp->{$mod} = $br->{$mod} if $br->{$mod} > $pp->{$mod};
    }
    else {
      $pp->{$mod} = $br->{$mod};
    }
  }
}

delete $WriteMakefileArgs{CONFIGURE_REQUIRES}
  unless eval { ExtUtils::MakeMaker->VERSION(6.52) };

WriteMakefile(%WriteMakefileArgs);



