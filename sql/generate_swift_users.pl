#!/usr/bin/perl -w 
use strict;
use DBD::mysql;
 
my $host = "localhost"; 
my $database = "vospace_20";
my $tablename = "storage_users_pool"; 
my $user = "root";
my $pw = "passwd"; 

my $dsn = "DBI:mysql:database=$database;host=$host"; 
my $dbh = DBI->connect($dsn, $user, $pw);

 
foreach(1..5){
	my $user = gen_rand(10); 
	my $passwd = gen_rand(15); 
 
	my $myquery = "INSERT INTO $tablename (username, apikey) VALUES (\"$user:$user\",\"$passwd\")"; 
 
	my $execute = $dbh->do($myquery);
	
	`swauth-add-user -A http://127.0.0.1:8081/auth/ -K keyauth -a $user $user $passwd`;
}
 
 
sub gen_rand { 
	my $length_of_randomstring=shift; # the length of
	# the random string to generate
 
	my @chars=('a'..'z','A'..'Z','0'..'9','_');
	my $random_string; 
	foreach (1..$length_of_randomstring) { 
		# rand @chars will generate a random 
		# number between 0 and scalar @chars 
		$random_string.=$chars[rand @chars]; 
	}
	return $random_string; 
}
