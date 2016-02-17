#!/usr/bin/perl

# This script is an attempt at parallelizing the db2 script export produced by db2tocopy
# For those who are in a hurry

# Just give it the path to the db2 script and the expected parallelism
# The db2 program HAS to be in the path


use strict;
use warnings;
use threads;
use Thread::Queue;
use File::Temp qw/ :POSIX /;;
use POSIX qw(setsid);
my $q = Thread::Queue->new();

my $filename=$ARGV[0];
my $parallelism=$ARGV[1];


sub db2worker
{
	my ($param)=@_;
	# Create a db2 command to pipe into it
	while (defined(my $item = $q->dequeue_nb())) {
		my $tmpfile=tmpnam();
		open my $out,">>","$tmpfile" or die "Cannot open $tmpfile for writing: $!";
		print $out $param;
		print $out $item;
		close $out;
		# This is a bit ridiculous, but for now I don't know of another way to have more than one db2 command working simultaneously
		# db2 probably identifies the session by tty, creating a new tty is bothersome, let's use su
		my $rv=system ("su root -c \"db2 -f $tmpfile\"");
		if ($rv >> 8)
		{
			print "Error performing $item\n";
		}
		unlink $tmpfile;
	}
}




open my $fh,'<',$filename or die "Cannot open $filename, $!";

my $db2command=<$fh>;

my @threads;
for (my $i=0;$i<$parallelism;$i++)
{
	my $thread=threads->create('db2worker',$db2command);
	push @threads,($thread);
}

# Enqueue the work
while (my $work=<$fh>)
{
	$q->enqueue($work);
}


foreach my $thread (@threads)
{
	$thread->join();
}


