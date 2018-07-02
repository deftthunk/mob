#!/usr/bin/perl

## 4/29/09
##
##
## mob: A test project for multi-threaded parsing in kat
##
## **Usage**
##
##  sh$ mob <regex pattern> <LOG FILE(S)>
##  sh$ (z)cat <LOG FILE(S)> | mob <regex pattern>
##
## **Other stuff**
##
## -if giving an IP address, mob will automatically optimize it for regex
## -when providing a regex, it is a good idea to put it in single quotes
## -mob can be pointed to log files or fed them through a pipe from STDIN.
##  using a pipe will disable multi-threading abilities.
## -mob can take both compressed (gzip) and uncompressed files at same time
## -because disk I/O is the biggest bottleneck, CPU waiting time goes up
##  significantly when parsing uncompressed files. Performance is much
##  better all around if parsing compressed files, as they hit memory faster.
##

use strict;
use warnings;
use IO::Zlib;
use threads;
use Thread::Queue;
$| = 1;

## set max concurrent threads
my $max_threads = 10;

## nice the script (and spawned child threads/processes) so we don't hog resources
setpriority( 0, $$, 10 );

sub get_input {
  my @input;
  my $pattern = shift @ARGV;

  if( -p STDIN ) {
    push( @input, \'stdin' );
  }
  else {
    foreach ( @ARGV ) {
      my $file;
      if( $_ =~ /.*\.gz$/ ) {
        open( $file, "gzip -dc $_ |" ) or die( "Error: Cannot open '$_'\n" );
      }
      else {
        open( $file, "< $_" ) or die( "Error: Cannot open '$_'\n" );
      }
      push( @input, \$file );
    }
  }
  return( \@input, $pattern );
}

sub parse {
  my $fh = ${${$_[0]}};
  my $pattern = $_[1];
  my $t_queue = ${$_[2]};
  my @data;

  if( $fh eq 'stdin' ) {  ## if STDIN (pipe), execute
    while( <STDIN> ) {
      if( $_ =~ $pattern ) {
        print $_;
      }
    }
  }
  else {
    while( <$fh> ) {
      if( $_ =~ $pattern ) {
        push( @data, $_ );
      }
    }
  }

  close $fh;
  $t_queue->dequeue_nb;  ## signal thread completion by removing
              ## an element from the shared queue
  return \@data;
}

sub main {
  my( $input, $pattern ) = get_input();
  my $regex;

  ## optimize IP address patterns for regex compilation
  if( $pattern =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/ ) {
    $pattern =~ s/\./\\\./g;
    $regex = qr/\D$pattern/;
  }
  else {
    $regex = qr/$pattern/;
  }
  my @thr_array;
  my $counter = 0;
  my $queue = new Thread::Queue;  ## put an element on the queue to signal
                  ## an active thread
  foreach( @{$input} ) {
    push( @thr_array, threads->create( \&parse, \$_, $regex, \$queue ) );
    $queue->enqueue("placeholder");
      
    if( $queue->pending > $max_threads ) {
      while( $queue->pending > $max_threads ) {
        sleep 0.001;
      }

      ## for each thread, print data, clean up and close properly
      my $ReturnData = $thr_array[$counter]->join();
      print @{$ReturnData}, "\n";
      delete $thr_array[$counter];
      $counter++;
    }
  }

  ## same as above, but just catching remaining threads before closing program
  foreach( $counter .. $#thr_array ) {
    my $ReturnData = $thr_array[$_]->join();
    print @{$ReturnData}, "\n";
  }
}

main();
exit;

