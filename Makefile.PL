
use strict;
use warnings;



use ExtUtils::MakeMaker 6.30;



my %WriteMakefileArgs = (
  "ABSTRACT" => "Redis-backed library for creating background jobs, placing them on multiple queues, and processing them later.",
  "AUTHOR" => "Diego Kuperman <diego\@freekeylabs.com>",
  "BUILD_REQUIRES" => {
    "Exporter" => 0,
    "File::Temp" => 0,
    "Test::More" => "0.96",
    "base" => 0,
    "warnings" => 0
  },
  "CONFIGURE_REQUIRES" => {
    "ExtUtils::MakeMaker" => "6.30"
  },
  "DISTNAME" => "Resque",
  "EXE_FILES" => [],
  "LICENSE" => "perl",
  "NAME" => "Resque",
  "PREREQ_PM" => {
    "Any::Moose" => 0,
    "Carp" => 0,
    "DateTime" => 0,
    "JSON" => 0,
    "List::MoreUtils" => 0,
    "POSIX" => 0,
    "Redis" => 0,
    "Scalar::Util" => 0,
    "Sys::Hostname" => 0,
    "Try::Tiny" => 0,
    "UNIVERSAL::require" => 0,
    "overload" => 0,
    "strict" => 0
  },
  "VERSION" => "0.04",
  "test" => {
    "TESTS" => "t/*.t"
  }
);


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



