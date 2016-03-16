#!/usr/bin/env perl
#===============================================================================
#
#         FILE: db2topg.pl
#
#        USAGE: ./db2topg.pl  
#
#  DESCRIPTION: Convert a DB2 SQL schema dump to a PostgreSQL dump
#
#===============================================================================

use strict;
use warnings;
use utf8;

use Carp;

my $schema_db2;

use Data::Dumper;
use Getopt::Long;
use Encode::Guess; # No idea what encoding DB2 will use for its dump (for objects with diacritic characters)

# Migrate DB2 to PostgreSQL

# Global variables (command line arguments)
my $filename;
my $help;
my $do_tablespaces=0;
my $data_directory;
my $data_script_type;
my $db2dbname;
my $db2username;
my $db2password;

sub read_and_cleanup_line
{
	my $line=<IN>;
	return undef if (not defined $line);
	$line=~ s/--.*//;
	return $line;
}

sub read_statement
{
	my @statement;
	my $read;
	my $seen_end_function=0;
	while (my $line=read_and_cleanup_line())
	{
		next if ($line=~/^\s*$/);
		$read=1;
		push @statement,($line);
		# We have an exception for create functions: there will be semi-columns inside…
		if ($statement[0] !~ /CREATE.*FUNCTION/i)
		{
			last if ($line=~/;\s*$/);
		}
		else
		{
			# There are functions containing SQL, others containing a sort of plpgsql
			# If the function contains a begin atomic, lets suppose it's sort of plpgsql
			if (scalar(grep(/^\s*begin\s+atomic\s*/i,@statement))==0)
			{
				# Stop as soon as there is a semicolon
				last if ($line=~/;\s*$/);
			}
			else
			{
				# We stop when we have seen the "end" keyword, and the semi colon after it
				if ($line =~ /end;?\s*$/)
				{
					$seen_end_function=1;
				}
				if ($seen_end_function)
				{
					last if ($line=~/;\s*$/);
				}
			}
		}
	}
	if ($read)
	{
		# Cleanup trailing semi-colon
		$statement[-1]=~ s/;$//;
		return \@statement;
	}
	return undef; # Behave like read: return undef if reads nothing
}

# Reads all remaining lines in a statement
sub slurp_statement
{
	my ($refstatement)=@_;
	my $statement='';
	while(my $line=shift(@$refstatement))
	{
		$statement.=$line;
	}
	$statement=~ s/;$//s;

	return $statement;
}

# Slurp the rest of a comment, and remove trailing quote
sub slurp_comment
{
	my ($refstatement)=@_;
	my $comment=slurp_statement($refstatement);
	$comment =~ s/'$//si;

	return $comment;
	
}

# With DB2, one can specify «with default», with no default value. If that's the case, there is a «default» default value.
# This function should be called when you need to find the default value to a type. It is returned as the SQL litteral to be used.
sub find_default_default
{
	my ($type)=@_;
	if ($type =~ /^(SMALLINT|INT|BIGINT|DECIMAL|NUMERIC|REAL|DOUBLE|DECFLOAT|FLOAT)/i)
	{
		return 0;
	}
	if ($type =~ /^(CHAR|GRAPHIC)/i)
	{
		return "''";
	}
	if ($type =~ /^(VARCHAR|CLOB|VARGRAPHIC|DBCLOB|VARBINARY|BLOB)/i)
	{
		return "''";
	}
	if ($type =~ /^DATE/i)
	{
		return 'current_date';
	}
	if ($type =~ /^TIME/i)
	{
		return 'current_time';
	}
	if ($type =~ /^TIMESTAMP/i)
	{
		return 'current_timestamp';
	}
	die "Unknown type $type when trying to find 'default' default value for a type\n";
}

# Convert DB2's peculiar types to PostgreSQL
sub convert_type
{
	my ($in_type)=@_;
	my $out_type=$in_type; # Most of the time, this is enough
	# CLOB and BLOB can have more specifications (logged, compact, etc…). We ignore them completely
	if ($in_type =~ /^BLOB\((\d+)\)/)
	{
		$out_type="bytea"; # Could add a check constraint to verify size, but most of the time, the size is here 
	}
	elsif ($in_type =~ /^CLOB\((\d+)\)/)
	{
		$out_type="varchar($1)"; # That's just a varchar to us, as these can store up to 1GB
	}
	elsif ($in_type eq 'DOUBLE')
	{
		$out_type='double precision';
	}
	elsif ($in_type eq 'LONG VARCHAR')
	{
		$out_type='text';
	}
	return $out_type;
}

# Some keywords are reserved in PostgreSQL, such as //TABLE//. We'll need to protect them with double quotes
my %reserved_keywords=('TABLE'  => 1,
					   'UNIQUE' => 1,
					   'DISTINCT' =>1,
);
# The one arg version. Called by the main function for each element of the array
sub _protect_reserved_keywords
{
	my ($kw)=@_;
	croak unless (defined $kw);
	
	
	# First store the ASC/DESC somewhere if there is one
	my $ascdesc='';
	if ($kw =~ s/(\s+(?:ASC|DESC))//)
	{
		$ascdesc=$1;
	}
	# Also remove minus in object name… they are not supported in PostgreSQl (and in the SQL standard either i think)
	if ($kw =~ /-/)
	{
		print STDERR "I had to rename $kw as it contained a '-' sign. Removing it\n";
		$kw=~ s/-//g;
	}
	# Is this a reserved keyword ? Quote it if yes
	if (exists($reserved_keywords{$kw}))
	{
		$kw = '"' . $kw . '"';
	}
	# Put back the ascdesc at the end
	$kw .= $ascdesc;
	
	return lc($kw); # FIXME: change this if we want to do a case sensitive schema
}

sub protect_reserved_keywords
{
	my @result=map{_protect_reserved_keywords($_)} @_;
	if ($#result==0)
	{
		# Called with only one value. Expects a scalar
		return $result[0];
	}
	return @result;
}

# Try to fix some expressions (for default values for example)
# Only does brutal regexp corrections
sub try_fix_expression
{
	my ($data)=@_;
	# get rid of newlines, anyway we just want to import the objects (views), they will be reformatted by PG anyway
	$data =~ s/\r?\n/ /g;
	# Date retrieval
	$data =~ s/\bcurrent\s+date/current_date/gi;
	$data =~ s/\bcurrent\s+timestamp/current_timestamp/gi;
	# Time conversions
	$data =~ s/\byear\(/extract (YEAR FROM /gi;
	# Case conversions
	$data =~ s/\bUCASE\(/upper(/gi;
	$data =~ s/\bLCASE\(/lower(/gi;
	# Type conversions
	$data =~ s/\bCHAR\(/to_char(/gi;
	
	# for an empty blob:
	if ($data =~ /"SYSIBM"."BLOB"/)
	{
		$data="''"; # Automatically casted in PostgreSQL, no need!
	}
	
	# The rest
	$data =~ s/WITH ROW MOVEMENT|WITH NO ROW MOVEMENT//i; # No meaning anyway in PG
	return $data;
}

# Checks there is no object with the same name (sequence, table, index…). And warns and renames if these is a problem.
# In DB2, objects are namespaced by table (you can have the same index or constraint name for instance, on two different tables)
# Names will be protected as necessary
my %renames; # Contains all the object names (sequences, tables, indexes…) I had to create to avoid conflicts
sub check_and_rename
{
	my ($schema,$name,$type)=@_;;
	
	# First, populate %renames if this is the first call, with the table names (these wont move)
	unless (%renames)
	{
		foreach my $schema(keys %{$schema_db2->{SCHEMAS}})
		{
			foreach my $table(keys %{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}})
			{
				$renames{protect_reserved_keywords($schema)}->{protect_reserved_keywords($table)}='TABLE';
			}
		}
	}
	$schema=protect_reserved_keywords($schema);
	$name=protect_reserved_keywords($name);
	# Ok, we got called, 
	# We first try the name without modification
	# Then we try to add the object_type at the end of the name
	# Then we add a number at the end of its name in a loop until it does not conflict (start with an empty string)
#	print STDERR Dumper(\%renames);
#	print STDERR "NEW OBJECT : $schema , $name , $type\n";
	unless (exists $renames{$schema}->{$name})
	{
		$renames{$schema}->{$name}=$type;
		return $name;
	}
	# We have a conflict. Give a try to the name plus type
	unless (exists $renames{$schema}->{$name.'_'.$type})
	{
		$renames{$schema}->{$name.'_'.$type}=$type;
		print STDERR "I had to rename the $type $schema.$name to $schema.${name}_${type} to avoid conflict\n";
		return $name.'_'.$type;
	}
	my $id=1;
	while (exists $renames{$schema}->{$name.$id})
	{
		$id++;
	}
	$renames{$schema}->{$name.$id}=$type;
	print STDERR "I had to rename the $type $schema.$name to $schema.${name}${id} to avoid conflict\n";

	return $name.$id;
}



my $current_schema=''; # Global as this will be set on a per view afterwards
my $current_path=''; # Global as this will be set on a per view afterwards
sub parse_dump
{
	my ($filename)=@_;
	# First guess encoding
	open IN, '<',$filename or die "Cannot open $filename, $!";
	my $data_guess;
    while (my $line = <IN>)
    {
        $data_guess .= $line;
    }
    close IN;
	


    # We now ask guess...
    my $decoder = guess_encoding($data_guess, qw/iso8859-15 utf8 utf16-le utf16-be/);
    die $decoder unless ref($decoder);	

#	print "encoding: " . $decoder->name . "\n";
	
	open IN, "<:encoding(".$decoder->name.")",$filename or die "Cannot open $filename, $!";
	MAIN:
	while (my $refstatement=read_statement)
	{
		# New statement to parse
		# Determine the statement type
		my $line=shift(@$refstatement);
		next if ($line =~ /^CREATE BUFFERPOOL/);
		next if ($line =~ /^CONNECT (TO|RESET)/);
		next if ($line =~ /^ALTER TABLESPACE/);
		next if ($line =~ /^COMMIT WORK/);
		next if ($line =~ /^TERMINATE/);
			if ($line =~ /^CREATE (?:REGULAR|LARGE|(?:USER )?TEMPORARY) TABLESPACE "(.*?)\s*"/)
		{
			# Parse tablespace
			my $name=$1;
			TABLESPACE:
			while (my $line=shift(@$refstatement))
			{
				# Read the rest of create tablespace
				if ($line =~ /^\s+USING \((?:FILE )?'(.*)'(?: \d+)?(\)|,)$/)
				{
					push @{$schema_db2->{TABLESPACE}->{$name}->{PATH}},($1);
					if ($2 eq ',')
					{
						# There are other files in this tablespace
						while (my $line=shift(@$refstatement))
						{
							$line =~ /^\s+(?:FILE )?'(.*)'(?: \d+)?(,|\))/ or die "I don't understand the list of files in this tablespace";
							push @{$schema_db2->{TABLESPACE}->{$name}->{PATH}},($1);
							if (defined $2)
							{
								# no more files
								next TABLESPACE;
							}
						}
					}
					else
					{
						next;
					}
				}
				next if ($line =~ /EXTENTSIZE|PREFETCHSIZE|BUFFERPOOL|OVERHEAD|TRANSFERRATE|AUTORESIZE|INCREASESIZE|MAXSIZE|FILE SYSTEM CACHING|DROPPED TABLE/);
				die "I don't understand $line in a CREATE TABLESPACE section";
			}
		} #CREATE TABLESPACE
		elsif ($line =~ /^CREATE ROLE "(.*?)\s*"$/)
		{
			my %empty_hash=();
			$schema_db2->{ROLES}->{$1}=\%empty_hash;
			die ("Overflow in create role: " . join('',@$refstatement)) unless ($#$refstatement == -1);
		
		}
		elsif ($line =~ /^COMMENT ON ROLE "(.*?)\s*" IS '(.*?)'?$/)
		{
			die "This role $1 hasn't been seen before" unless (exists $schema_db2->{ROLES}->{$1});
			$schema_db2->{ROLES}->{$1}->{COMMENT}=$2 . "\n" . slurp_comment($refstatement);
			chomp $schema_db2->{ROLES}->{$1}->{COMMENT};
		}
		elsif ($line =~ /^CREATE SCHEMA "(.*?)\s*"\s+AUTHORIZATION\s+"(.*)\s*"\s*$/)
		{
			$schema_db2->{SCHEMAS}->{$1}->{AUTHORIZATION}=$2;
			# Some roles may be there, and not have been created. I don't know why db2 would do this, but take care of it…
			unless (exists $schema_db2->{ROLES}->{$2})
			{
				my %empty_hash=();
				$schema_db2->{ROLES}->{$2}=\%empty_hash;
			}
			die ("Overflow in create schema: " . join('',@$refstatement)) unless ($#$refstatement == -1);
		}
		elsif ($line =~ /^CREATE SEQUENCE "(.*?)\s*"\."(.*?)\s*" AS INTEGER$/)
		{
			my $schema=$1;
			my $sequence=$2;
			while (my $line=shift(@$refstatement))
			{
				if ($line =~ /MINVALUE (\d+) MAXVALUE (\d+)/)
				{
					$schema_db2->{SCHEMAS}->{$schema}->{SEQUENCES}->{$sequence}->{MINVALUE}=$1;
					$schema_db2->{SCHEMAS}->{$schema}->{SEQUENCES}->{$sequence}->{MAXVALUE}=$2;
				}
				elsif ($line =~ /START WITH (\d+) INCREMENT BY (\d+)/)
				{
					$schema_db2->{SCHEMAS}->{$schema}->{SEQUENCES}->{$sequence}->{STARTWITH}=$1;
					$schema_db2->{SCHEMAS}->{$schema}->{SEQUENCES}->{$sequence}->{INCREMENTBY}=$2;
				}
				elsif ($line =~ /CACHE (\d+) (NO )?CYCLE/)
				{
					$schema_db2->{SCHEMAS}->{$schema}->{SEQUENCES}->{$sequence}->{CACHE}=$1;
					if (defined $2)
					{
						$schema_db2->{SCHEMAS}->{$schema}->{SEQUENCES}->{$sequence}->{CYCLE}=0;
					}
					else
					{
						$schema_db2->{SCHEMAS}->{$schema}->{SEQUENCES}->{$sequence}->{CYCLE}=1;
					}
				}
				else
				{
					die "I don't understand $line in a CREATE SEQUENCE section";
				}
			}
		} #CREATE SEQUENCE
		elsif ($line =~ /^ALTER SEQUENCE "(.*?)\s*"\."(.*?)\s*" RESTART WITH (\d+)$/)
		{
			$schema_db2->{SCHEMAS}->{$1}->{SEQUENCES}->{$2}->{RESTARTWITH}=$3;
			die ("Overflow in alter sequence: " . join('',@$refstatement)) unless ($#$refstatement == -1);
		}
		elsif ($line =~ /^CREATE TABLE "(.*?)\s*"\."(.*?)\s*"\s+\(\s*$/)
		{
			# Create table, multi-line of course
			my $schema=$1;
			my $table=$2;
			my $incols=1;
			my $colnum=0;
			while (my $line=shift(@$refstatement))
			{
				$colnum++;
				if ($line =~ /^\s+"(.*?)\s*"\s+(.+?)( NOT NULL)?(?: WITH DEFAULT (.*?)| GENERATED (BY DEFAULT|ALWAYS) AS IDENTITY \(| GENERATED (BY DEFAULT|ALWAYS) AS \((.*?)\))?( ,| \))?\s*$/)
				{
					my ($colname,$coltype,$colnotnull,$coldefault,$colgeneratedbydefaultidentity,$colgeneratedbydefaultexpression,$colgeneratedbydefaultexpressionAS,$endofline)=($1,$2,$3,$4,$5,$6,$7,$8);
					$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{TYPE}=convert_type($coltype);
					$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{ORIGTYPE}=$coltype;
					$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{COLNUM}=$colnum;
					if (defined ($colnotnull))
					{
						$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{NOTNULL}=1;
					}
					else
					{
						$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{NOTNULL}=0;
					}
					if (defined ($coldefault))
					{
						$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{DEFAULT}=$4;
					}
					if (defined ($endofline) and $endofline eq ')')
					{
						# End of the columns definition
						$incols=0;
					}
					if (defined ($colgeneratedbydefaultidentity) and not (defined ($colgeneratedbydefaultexpression))) # Seems there is a bug in certain versions of perl, capturing the GENERATED two times
					{			
						if ($colgeneratedbydefaultidentity eq 'BY DEFAULT')
						{
							$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{ALWAYS}=0;
						}
						else
						{
							$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{ALWAYS}=1;
						}
						# We have an identity (it's like a record with a sequence with PostgreSQL)
						# It's multi-line. Let's read the rest. There is always the exact same records
						while (my $line=shift(@$refstatement))
						{
							$line =~ /START WITH \+(\d+)|INCREMENT BY \+(\d+)|MINVALUE \+(\d+)|MAXVALUE \+(\d+)|(NO CYCLE)|(NO )?CACHE (\d+)?|(NO ORDER \) ,)/ or die "Cannot understand $line in an IDENTITY definition";
							if (defined ($1))
							{
								$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{STARTWITH}=$1;
							}
							if (defined ($2))
							{
								$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{INCREMENTBY}=$2;
							}
							if (defined ($3))
							{
								$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{MINVALUE}=$3;
							}
							if (defined ($4))
							{
								$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{MAXVALUE}=$4;
							}
							if (defined ($5))
							{
								$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{CYCLE}=0;
							}
							if (defined ($6))
							{
								# No cache… means cache=1 under PostgreSQL
								$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{CACHE}=1;
							}
							if (defined ($7))
							{
								$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{CACHE}=$7;
							}
							if (defined ($8))
							{
								$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{ORDER}=0;
								last; # Finished reading the identity definition
							}
						}

					}
					elsif (defined ($colgeneratedbydefaultexpression))
					{
						# This is a generated column with a function. We'll put a default value instead
						# There is no way to emulate the GENERATED ALWAYS. Only warn.
						if ($colgeneratedbydefaultexpression ne 'BY DEFAULT')
						{
							print STDERR "==>Warning: column $colname of table $schema.$table has a default value using GENERATE ALWAYS. This can't be done with PostgreSQL. It will be a default value<==\n";
						}
						else
						{
							$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{IDENTITY}->{ALWAYS}=1;
						}
						$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COLS}->{$colname}->{DEFAULT}=$colgeneratedbydefaultexpressionAS;
						print STDERR "==>Warning: column $colname of table $schema.$table has a default value using an expression. This may not work... you may have to correct this manually, and write a trigger<==\n";
					}
				}
				elsif ($line =~ /^\s*(?:IN "(.*?)\s*")? *(?:INDEX IN "(.*?)\s*")? *(?:LONG IN "(.*?)\s*")?\s*;?\s*$/)
				{
						$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{TBSTABLE}=$1 if (defined $1);
						$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{TBSINDEX}=$2 if (defined $2);
						$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{TBSLONG}=$3 if (defined $3);
				}
				else
				{
					die "I don't understand $line in a CREATE TABLE section";
				}
			}
		} # CREATE TABLE
		elsif ($line =~ /^ALTER TABLE "(.*?)\s*"\."(.*?)\s*"\s*$/)
		{
			my $schema=$1;
			my $table=$2;
			my $line=shift(@$refstatement);
			if ($line=~/^\s+ADD(?: CONSTRAINT "(.*?)\s*"\s*)? (PRIMARY KEY|UNIQUE)$/)
			{
				my %object;
				my $type=$2;
				if (defined $1)
				{
					$object{NAME}=$1;
				}
				# Read the column list
				while (my $line=shift(@$refstatement))
				{
					if ( $line =~ /^\s+\(?(\S+)(,|\))/)
					{
						push @{$object{COLS}},($1);
					}
					else
					{
						die "I don't understand $line in an ALTER TABLE section";
					}
				}
				if ($type eq 'PRIMARY KEY')
				{
					$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{PK}=\%object;
				}
				else
				{
					$object{TYPE}='UNIQUE';
					push @{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{CONSTRAINTS}},(\%object);
				}
			} # Primary/Unique
			elsif ($line=~/^\s+ADD(?: CONSTRAINT "(.*?)\s*"\s*)? FOREIGN KEY\s+$/)
			{
				my %object;
				$object{TYPE}='FK';

				if (defined $1)
				{
					$object{NAME}=$1;
				}
				# Read the local column list
				while (my $line=shift(@$refstatement))
				{
					if ( $line =~ /^\s+\(?(\S+)(,|\))/)
					{
						push @{$object{LOCALCOLS}},($1);
					}
					else
					{
						# End of this part. Now to the REFERENCES part of the constraint
						unshift @$refstatement,($line);
						last;
					}
				}
				# Read the foreign table name
				$line=shift(@$refstatement);
				$line=~ /^\s+REFERENCES "(.*?)\s*"\."(.*?)\s*"\s*$/ or die "I don't understand $line in an ALTER TABLE section";
				$object{FKTABLE}=$2;
				$object{FKSCHEMA}=$1;
				# Read the remote column list
				while (my $line=shift(@$refstatement))
				{
					if ( $line =~ /^\s+\(?(\S+)(,|\))/)
					{
						push @{$object{REMOTECOLS}},($1);
					}
					else
					{
						# End of this part. Now to the rest of the constraint
						unshift @$refstatement,($line);
						last;
					}
				}
				while (my $line=shift(@$refstatement))
				{
					if ($line =~/^\s+(?:ON (DELETE|UPDATE) (RESTRICT|NO ACTION|CASCADE)|(ENFORCED)|(ENABLE QUERY OPTIMIZATION))\s*;?$/)
					{
						if (defined $3)
						{
							$object{ENFORCED}=1
						}
						elsif (defined $4)
						{
							next;
						}
						else
						{
							# It's an on delete/on update
							$object{"ON".$1}=$2;
						}
					}
					else
					{
						die "I don't understand $line in an ALTER TABLE FOREIGN KEY SECTION"
					}
				}
				# We got there, the whole FK is parsed. Store it
				push @{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{CONSTRAINTS}},(\%object);
			} # FK
			elsif ($line=~/^\s+ADD(?: CONSTRAINT (\S+))? CHECK\s+$/)
			{
				# Next lines is the declaration of the constraint, until we reach ENFORCED
				my %object;
				$object{TYPE}='CHECK';

				if (defined $1)
				{
					$object{NAME}=$1;
				}
				my @check_code;
				while (my $line=shift(@$refstatement))
				{
					if ($line =~/^\s+ENFORCED\s*$/)
					{
						unshift @$refstatement,($line);
						last; # We have read this definition
					}
					# Remove tabs and spaces from input
					$line =~ s/^\s*//;
					$line =~ s/\s*$//;
					push @check_code,($line);
					# FIXME: maybe the code should check there is nothing but ENFORCED and ENABLE QUERY OPTIMIZATION ?
				}
				$object{CODE}.=join(' ',@check_code);
				chomp $object{CODE}; # Remove the trailing \n
				push @{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{CONSTRAINTS}},(\%object);
			} # CHECK
			else
			{
				die "I don't understand $line in an ALTER TABLE section";
			}
		} # ALTER TABLE
		elsif ($line =~ /^ALTER TABLE .* PCTFREE \d+/)
		{
			# No point in keeping this. It exists in PG too, but the reasoning in setting it is entirely different.
			next;

		}
		elsif ($line =~ /^ALTER TABLE "(.*?)\s*"\."(.*?)\s*" ALTER COLUMN "(.*?)\s*" RESTART WITH (\d+)\s*$/)
		{
			$schema_db2->{SCHEMAS}->{$1}->{TABLES}->{$2}->{COLS}->{$3}->{IDENTITY}->{STARTWITH}=$4;
		}
		elsif ($line =~ /^CREATE (UNIQUE )?INDEX "(.*?)\s*"\."(.*?)\s*" ON "(.*?)\s*"\."(.*?)\s*"\s*$/)
		{
			my ($indexschema,$indexname,$tableschema,$tablename)=($2,$3,$4,$5);
			# We ignore indexschema… it doesn't exist in PostgreSQl anyway
			if (defined $1 and $1 eq 'UNIQUE ')
			{
				$schema_db2->{SCHEMAS}->{$tableschema}->{TABLES}->{$tablename}->{INDEXES}->{$indexname}->{UNIQUE}=1;
			}
			else
			{
				$schema_db2->{SCHEMAS}->{$tableschema}->{TABLES}->{$tablename}->{INDEXES}->{$indexname}->{UNIQUE}=0;
			}
			# Read the column list
			while (my $line=shift(@$refstatement))
			{
				# column list. there may be asc/desc
				if ( $line =~ /^\s+\(?(\S+(?:\s+\S+))(,|\);?)/)
				{
					push @{$schema_db2->{SCHEMAS}->{$tableschema}->{TABLES}->{$tablename}->{INDEXES}->{$indexname}->{COLS}},($1);
					last if ($2 eq ')');# End of the list of columns
				}
				else
				{
					die "I don't understand $line in a create index. I expected a list of columns"
				}
			}

			# We may have an include definition
			$line=shift(@$refstatement);
			if ($line=~/INCLUDE \((\S+) (,|\));?$/)
			{
				push @{$schema_db2->{SCHEMAS}->{$tableschema}->{TABLES}->{$tablename}->{INDEXES}->{$indexname}->{INCLUDECOLS}},($1);
				if ($2 eq ',') # There are more columns
				{
					while (my $line=shift(@$refstatement))
					{
						if ( $line =~ /^\s+(\S+)\s*(,|\);?)/)
						{
							push @{$schema_db2->{SCHEMAS}->{$tableschema}->{TABLES}->{$tablename}->{INDEXES}->{$indexname}->{INCLUDECOLS}},($1);
							last if ($2 eq ')');# End of the list of columns of the include
						}
						else
						{
							die "I don't understand $line in create index. I expected a list of columns for an include section"
						}
					}
				}
				# We have finished the INCLUDE. Read another line in case there is something else
				next unless ($line=shift(@$refstatement));
			}
			if ($line =~ /(?:DIS)?ALLOW REVERSE SCANS/)
			{
				die ("Overflow in comment on column: " . join('',@$refstatement)) unless ($#$refstatement == -1);
			}
			else
			{
				die "I don't understand $line in an CREATE INDEX section";
			}

		}
		elsif ($line =~ /^COMMENT ON COLUMN "(.*?)\s*"\."(.*?)\s*"\."(.*?)\s*"\s* IS '(.*?)'?$/)
		{
			my $comment=$1;
			# Is this really a table, or is it a view (yeah, db2…)
			if (exists $schema_db2->{SCHEMAS}->{$1}->{TABLES}->{$2})
			{
				$schema_db2->{SCHEMAS}->{$1}->{TABLES}->{$2}->{COLS}->{$3}->{COMMENT}=$4 . "\n" . slurp_comment($refstatement);
				chomp $schema_db2->{SCHEMAS}->{$1}->{TABLES}->{$2}->{COLS}->{$3}->{COMMENT};
			}
			else
			{
				$schema_db2->{SCHEMAS}->{$1}->{VIEWS}->{$2}->{COLS}->{$3}->{COMMENT}=$4 . "\n" . slurp_comment($refstatement);
				chomp $schema_db2->{SCHEMAS}->{$1}->{VIEWS}->{$2}->{COLS}->{$3}->{COMMENT};
			}
		}
		elsif ($line =~ /^COMMENT ON TABLE "(.*?)\s*"\."(.*?)\s*"\s* IS '(.*?)'?$/)
		{
			# Is this really a table, or is it a view (yeah, db2…)
			if (exists $schema_db2->{SCHEMAS}->{$1}->{TABLES}->{$2})
			{
				$schema_db2->{SCHEMAS}->{$1}->{TABLES}->{$2}->{COMMENT}=$3 . "\n" . slurp_comment($refstatement);
				chomp $schema_db2->{SCHEMAS}->{$1}->{TABLES}->{$2}->{COMMENT};
			}
			else
			{
				$schema_db2->{SCHEMAS}->{$1}->{VIEWS}->{$2}->{COMMENT}=$3 . "\n" . slurp_comment($refstatement);
				chomp $schema_db2->{SCHEMAS}->{$1}->{VIEWS}->{$2}->{COMMENT};
			}
		}
		elsif ($line =~ /CREATE DISTINCT TYPE "(.*?)\s*"."(.*?)\s*" AS "SYSIBM  ".(.*)/)
		{
			# Only manage this case for now: this isn't a type, it's a domain
			$schema_db2->{SCHEMAS}->{$1}->{DOMAINS}->{$2}->{BASETYPE}=convert_type($3);
		}
		elsif ($line =~ /^SET CURRENT SCHEMA = "(.*?)\s*"\s*$/)
		{
			# Current schema. Probably for an incoming create view. Store it globally, we will add these info to the view
			$current_schema=$1;
		}
		elsif ($line =~ /^SET CURRENT PATH = (\S+)\s*$/)
		{
			# Current path. Probably for an incoming create view. Store it globally, we will add these info to the view
			$current_path=$1;
		}
		elsif ($line =~ /^CREATE VIEW (?:(\S+)\s*\.)?(\S+)\s*(.*?)$/i)
		{
			# A create view statement. We just slurp the rest of the definition. There is not a lot we can do, as these are dumped as is by db2
			# We don't store them by schema. We keep the order DB2 dumps them in, as some may depend on others
			my $schema;
			my $view=$2;
			my $definition=$3;
			if (defined $1)
			{
				# Oh, by the way, as this is exactly the input statement from the user when he created the view, case can be whatever… 
				# check if the schema starts with a double quote, who knows…
				$schema=$1;
# 				unless ($schema =~ /^"/)
# 				{
# 					$schema=uc($schema);
# 				}
			}
			else
			{
				$schema=$current_schema;
			}
			my $objview;
			$objview->{NAME}=$view;
			$objview->{STATEMENT}=$definition . "\n" . slurp_statement($refstatement);
			chomp ($objview->{STATEMENT});
			$objview->{SCHEMAS}=$schema;
			$objview->{CURRENT_SCHEMA}=$current_schema;
			$objview->{CURRENT_PATH}=$current_path;
			push @{$schema_db2->{VIEWS}},($objview);
		}
		elsif ($line =~ /^CREATE TRIGGER (\S+)\s*\.(\S+)\s*$/)
		{
			# Same as with views. Not a lot we can do about triggers, languages are too different
			# SQL is as input by the user, so let's take care about case
			my $schema=$1;
			my $trigger=$2;
			unless ($schema =~ /^"/)
			{
				$schema=uc($schema);
			}

			unless ($trigger=~ /^"/)
			{
				$trigger=uc($trigger);
			}

			$schema_db2->{SCHEMAS}->{$schema}->{TRIGGERS}->{$trigger}->{CURRENT_SCHEMA}=$current_schema;
			$schema_db2->{SCHEMAS}->{$schema}->{TRIGGERS}->{$trigger}->{CURRENT_PATH}=$current_path;
			$schema_db2->{SCHEMAS}->{$schema}->{TRIGGERS}->{$trigger}->{STATEMENT}= slurp_statement($refstatement);
		}
		elsif ($line =~ /^COMMENT ON TRIGGER "(.*?)\s*"\s*\."(.*?)\s*"\s* IS '(.*?)'?$/)
		{
			$schema_db2->{SCHEMAS}->{$1}->{TRIGGERS}->{$2}->{COMMENT}=$3 . "\n" . slurp_comment($refstatement);
			chomp $schema_db2->{SCHEMAS}->{$1}->{TRIGGERS}->{$2}->{COMMENT};
		}
		elsif ($line =~ /^CREATE FUNCTION (\S+)\.(\S+)/)
		{
			# These are functions. Languages are completely different
			my $schema=$1;
			my $function=$2;
			# There can be quotes and whatever in these, depending on how the person has created the function
			$schema =~ s/^"//;
			$schema =~ s/\s*"//;
			$function =~ s/^"//;
			$function =~ s/\s*"//;
			
			{
				$schema=uc($schema);
			}

			unless ($function=~ /^"/)
			{
				$function=uc($function);
			}
			$schema_db2->{SCHEMAS}->{$schema}->{FUNCTIONS}->{$function}->{CURRENT_SCHEMA}=$current_schema;
			$schema_db2->{SCHEMAS}->{$schema}->{FUNCTIONS}->{$function}->{CURRENT_PATH}=$current_path;
			$schema_db2->{SCHEMAS}->{$schema}->{FUNCTIONS}->{$function}->{STATEMENT}= slurp_statement($refstatement);

		}
		elsif ($line =~ /^GRANT/)
		{
			# The privilege system is too different. Just ignore it
			next;
		}
		else
		{
			die "I don't understand <$line>";
		}
	}
	close IN;
}

# Produce the SQL definition of a column (type and NULL/NOT NULL
sub get_coldef
{
	my ($colref)=@_;
	my $type=$colref->{TYPE};
	my $notnull=$colref->{NOTNULL};
	my $default=$colref->{DEFAULT};
	my $rv='';
	# There will be types conversion before too long
	$rv = $type;
	if ($notnull)
	{
		$rv .= ' NOT NULL';
	}
	if (defined($default))
	{
		if ($default eq '')
		{
			$rv .= ' DEFAULT ' . find_default_default($type); # This is a WITH DEFAULT without anything more
		}
		else
		{
			$rv .= ' DEFAULT ' . try_fix_expression($default);
		}
	}
	return $rv;
}

# Will create before, after and unsure (mostly views and triggers) files
sub produce_schema_files
{
	my $before_file=$data_directory . '/' . "before.sql";
	open BEFORE, '>:utf8', $before_file or die "Cannot open $before_file for writing, $!";
	print BEFORE "set client_encoding to UTF8;\n";
	
	# Do the AFTER file:
	my $after_file=$data_directory . '/' . "after.sql";
	open AFTER, '>:utf8', $after_file or die "Cannot open $before_file for writing, $!";
	print AFTER "set client_encoding to UTF8;\n";
	
	
	my $unsure_file=$data_directory . '/' . "unsure.sql";
	open UNSURE, '>:utf8', $unsure_file or die "Cannot open unsure_file for writing, $!";
	print UNSURE "-- This file probably won't work as is. Try to run it, catch errors, and try to correct it\n";
	print UNSURE "-- If there is an obvious improvement that can be done to its generation, please file a bug or provide a patch\n";
	print UNSURE " -- Only understood with 9.5+\n";
	print UNSURE "\\set ECHO errors\n";
	print UNSURE "set client_encoding to UTF8;\n";
	print UNSURE "set search_path TO 'db2,\"\$user\", public';\n";
	# Do the create tablespaces (if $do_tablespaces), and create table
	if ($do_tablespaces)
	{
		foreach my $tablespace	(keys(%{$schema_db2->{TABLESPACE}}))
		{
			# A tablespace is a path in PG. We take the first location provided by db2
			print BEFORE "CREATE TABLESPACE " . protect_reserved_keywords($tablespace) . " LOCATION '" . $schema_db2->{TABLESPACE}->{$tablespace}->{PATH}->[0] . "';\n";
		}
	}
	# Do the create roles
	foreach my $role (keys (%{$schema_db2->{ROLES}}))
	{
		print BEFORE "CREATE role " . protect_reserved_keywords($role) . ";\n";
		if (exists($schema_db2->{ROLES}->{$role}->{COMMENT}))
		{
			print BEFORE "COMMENT ON ROLE $role IS '" . $schema_db2->{ROLES}->{$role}->{COMMENT} . "';\n";
		}
		print BEFORE "\n";
	}

	# Do the create schemas
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		my $authorization='';
		if (exists $schema_db2->{SCHEMAS}->{$schema}->{AUTHORIZATION})
		{
			# Some authorizations may be missing: it happens when objects in the dump are in a schema for which
			# there is no create schema in it. Don't know why it happens though (and don't care :) )
			$authorization = " AUTHORIZATION " . $schema_db2->{SCHEMAS}->{$schema}->{AUTHORIZATION};
		}
		print BEFORE "CREATE SCHEMA " . protect_reserved_keywords($schema) . $authorization . ";\n\n";
	}
	
	# Do the create sequences
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		foreach my $sequence (keys(%{$schema_db2->{SCHEMAS}->{$schema}->{SEQUENCES}}))
		{
			my $sobj=$schema_db2->{SCHEMAS}->{$schema}->{SEQUENCES}->{$sequence};
			
			my $seqname=check_and_rename($schema,$sequence,'SEQUENCE');
			
			print BEFORE "CREATE SEQUENCE " , protect_reserved_keywords($schema),".", protect_reserved_keywords($seqname),
						" INCREMENT BY " , $sobj->{INCREMENTBY} , "\n",
						" MINVALUE " , $sobj->{MINVALUE} , " MAXVALUE " , $sobj->{MAXVALUE} , "\n",
						" START " , $sobj->{STARTWITH} , " CACHE " , $sobj->{CACHE} , "\n", ' ',
						$sobj->{CYCLE}?'':'NO' , " CYCLE;\n";
			if (exists $sobj->{RESTARTWITH})
			{
				my $start=$sobj->{RESTARTWITH};
				if ($sobj->{RESTARTWITH} < $sobj->{MINVALUE})
				{
					# PostgreSQL won't accept this (it's silly)
					print STDERR "==> Sequence $schema.$sequence has RESTARTWITH(",$sobj->{RESTARTWITH},") smaller than MINVALUE(",$sobj->{MINVALUE},")",
					             ". Initializing to ",$sobj->{MINVALUE},"\n";
					$start=$sobj->{MINVALUE};
				}
				print BEFORE "ALTER SEQUENCE ",protect_reserved_keywords($schema),".", protect_reserved_keywords($seqname),
				             " RESTART WITH ",$start,";\n";
			}
			
		}
	}
	# Do the create domains (before the tables…)
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		foreach my $domain (keys(%{$schema_db2->{SCHEMAS}->{$schema}->{DOMAINS}}))
		{
			print BEFORE "CREATE DOMAIN $domain AS ", $schema_db2->{SCHEMAS}->{$schema}->{DOMAINS}->{$domain}->{BASETYPE},
			";\n";
		}
	}
	print BEFORE "\n";
	
	# Do the create tables
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		foreach my $table (keys(%{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}}))
		{
			my $tobj=$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table};
			print BEFORE "CREATE TABLE " . protect_reserved_keywords($schema) . "." . protect_reserved_keywords($table) . " (\n";
			my @cols;
			# Sort by COLNUM
			foreach my $col(sort { $tobj->{COLS}->{$a}->{COLNUM} <=> $tobj->{COLS}->{$b}->{COLNUM} } keys(%{$tobj->{COLS}}))
			{
				push @cols,(protect_reserved_keywords($col) . ' ' . get_coldef($tobj->{COLS}->{$col}))
			}
			# indent columns
			@cols=map {"\t".$_} @cols;
			print BEFORE join (",\n",@cols);
			print BEFORE ")"; # End of list of columns
			if ($do_tablespaces)
			{
				print $table if (not exists $tobj->{TBSTABLE});
				print BEFORE "\nTABLESPACE " . protect_reserved_keywords($tobj->{TBSTABLE});
				# If the table has a specialized tablespace index, use it too
				if (exists $tobj->{TBSINDEX})
				{
					print BEFORE "\nUSING INDEX TABLESPACE " . protect_reserved_keywords($tobj->{TBSINDEX});
				}
			}
			print BEFORE ";\n\n"; # End of the statement
			


			
			# Do the comments on tables and columns
			if (exists $schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table}->{COMMENT})
			{
				print BEFORE "COMMENT ON TABLE " . protect_reserved_keywords($schema) . "." .
				             protect_reserved_keywords($table) . " IS '" . $tobj->{COMMENT} . "';\n";
			}
			foreach my $col(sort { $tobj->{COLS}->{$a}->{COLNUM} <=> $tobj->{COLS}->{$a}->{COLNUM} } keys(%{$tobj->{COLS}}))
			{
				if (exists $tobj->{COLS}->{$col}->{COMMENT})
				{
					print BEFORE "COMMENT ON COLUMN " . protect_reserved_keywords($schema) . "." .
					             protect_reserved_keywords($table) . "." . protect_reserved_keywords($col) .
								 " IS '" . $tobj->{COLS}->{$col}->{COMMENT} . "';\n";
				}
			}
			print BEFORE "\n";

		}
	}
	

	

	# CREATE PKs and UNIQUE
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		foreach my $table (keys(%{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}}))
		{
			my $tobj=$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table};
			# Create PK
			if (exists $tobj->{PK})
			{
				print AFTER "ALTER TABLE " . protect_reserved_keywords($schema),".",protect_reserved_keywords($table), " ADD";
				if (exists $tobj->{PK}->{NAME})
				{
					print AFTER " CONSTRAINT " . check_and_rename($schema,$tobj->{PK}->{NAME},'PK');
				}
				print AFTER " PRIMARY KEY (",join (',',protect_reserved_keywords(@{$tobj->{PK}->{COLS}})),");\n";
			}
			# Create UNIQUEs
			foreach my $constraint (@{$tobj->{CONSTRAINTS}})
			{
				next unless ($constraint->{TYPE} eq 'UNIQUE');
				print AFTER "ALTER TABLE " . protect_reserved_keywords($schema),".",protect_reserved_keywords($table), " ADD";
				if (exists $constraint->{NAME})
				{
					print AFTER " CONSTRAINT ",check_and_rename($schema,$constraint->{NAME},'UNIQUE');
				}
				print AFTER " UNIQUE (",join (',',protect_reserved_keywords(@{$constraint->{COLS}})),");\n";
			}
		}
	}
	
	# CREATE INDEXES. Beware the covering indexes (don't exist yet in PostgreSQL
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		foreach my $table (keys(%{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}}))
		{
			my $tobj=$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table};
			foreach my $index (keys %{$tobj->{INDEXES}})
			{
				my $iobj=$tobj->{INDEXES}->{$index};
				# PostgreSQL has no covering index. It means it cannot add columns to a unique index that aren't part of the UNIQUE constraint
				# So if we have a covering index, and it is unique, we will produce two indexes for now. If one of these days PostgreSQL has
				# covering indexes (patches are under way), change this
				
				print AFTER "CREATE ", $iobj->{UNIQUE}?'UNIQUE ':'' ,"INDEX ",
				            check_and_rename($schema,$index,'INDEX'),
				            ' ON ' . protect_reserved_keywords($schema),'.',protect_reserved_keywords($table),
				            ' (', join (',',protect_reserved_keywords(@{$iobj->{COLS}}));
				if (exists $iobj->{INCLUDECOLS})
				{
					# We have include columns. Two strategies:
					# If the index is unique, we create a second, not-unique index, and print a warning telling we have an additional index for this
					# If the index isn't unique, we warn that we have added the columns in the main index
					if ($iobj->{UNIQUE})
					{
						# We finish the index and create a new one
						print AFTER ");\n";
						print AFTER "CREATE INDEX ",
						            check_and_rename($schema,$index.'_cov1','INDEX'), 
									' ON ' . protect_reserved_keywords($schema),'.',protect_reserved_keywords($table),         
						            ' (',join (',',protect_reserved_keywords(@{$iobj->{COLS}})),',',join (',',protect_reserved_keywords(@{$iobj->{INCLUDECOLS}})),
						            ");\n";
						print STDERR "==> IMPORTANT: $index is a UNIQUE covering index. These don't exist yet in PostgreSQL. I replaced it with 2 indexes<==\n";
					}
					else
					{
						# We just append these columns. It will only incur a slight slowdown in this index's updates
						print AFTER ',',join (',',protect_reserved_keywords(@{$iobj}->{INCLUDECOLS})),");\n";
						print STDERR "==> IMPORTANT: $index is a covering index. As it is not unique, INCLUDE columns have been added as plain index columns<==\n";
					}
				}
				else
				{
					# Just finish the statement
					print AFTER ");\n";
				}
			}
		}
	}
	# CREATE FKs
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		foreach my $table (keys(%{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}}))
		{
			my $tobj=$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table};
			foreach my $constraint (@{$tobj->{CONSTRAINTS}})
			{
				next unless ($constraint->{TYPE} eq 'FK');
				# Map everything as is… FIXME: except enforced
				print AFTER "ALTER TABLE ",protect_reserved_keywords($schema),".",protect_reserved_keywords($table), " ADD";
				if (exists $constraint->{NAME})
				{
					print AFTER " CONSTRAINT " . protect_reserved_keywords($constraint->{NAME});
				}
				print AFTER " FOREIGN KEY (",join(",",protect_reserved_keywords(@{$constraint->{LOCALCOLS}})),") REFERENCES ",
				            protect_reserved_keywords($constraint->{FKSCHEMA}),'.',
				            protect_reserved_keywords($constraint->{FKTABLE})," (",
				            join(",",protect_reserved_keywords(@{$constraint->{REMOTECOLS}})),") ",
				            "ON DELETE ", $constraint->{ONDELETE}, " ON UPDATE ", $constraint->{ONUPDATE};
				# We will try to validate everything in unsure.sql
				print AFTER " NOT VALID";
				print AFTER ";\n";
				
				print UNSURE "ALTER TABLE ",protect_reserved_keywords($schema),".",protect_reserved_keywords($table), " VALIDATE CONSTRAINT ";
				print UNSURE protect_reserved_keywords($constraint->{NAME});
				print UNSURE ";\n";
			}
		}
	}

	# Do the identities (last, as we need indexes on tables to be built: the max function will be much faster
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		foreach my $table (keys(%{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}}))
		{
			my $tobj=$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table};
				# Do the identities if there are any. Mapped to PostgreSQL's serial (a sequence + a default value)
			foreach my $col(sort { $tobj->{COLS}->{$a}->{COLNUM} <=> $tobj->{COLS}->{$a}->{COLNUM} } keys(%{$tobj->{COLS}}))
			{
				next unless (exists $tobj->{COLS}->{$col}->{IDENTITY});
				my $iobj=$tobj->{COLS}->{$col}->{IDENTITY};
				if ($iobj->{ORDER})
				{
					print STDERR "==> Sequences in PostgreSQL don't have an ORDER restriction, and $schema.$col has an identity that is ORDER. Behavior will be different<==\n";
				}
				if ($iobj->{ALWAYS})
				{
					print STDERR "==> Sequences in PostgreSQL don't have an ALWAYS restriction, and $schema.$col has an identity that is ALWAYS. Behavior will be different<==\n";
				}
				my $seqname=check_and_rename($schema,$table."_".$col."_seq",'SEQUENCE');
				print BEFORE "CREATE SEQUENCE " , protect_reserved_keywords($schema),".", protect_reserved_keywords($seqname),
				             " INCREMENT BY " , $iobj->{INCREMENTBY} , "\n",
				             " MINVALUE " , $iobj->{MINVALUE} , " MAXVALUE " , $iobj->{MAXVALUE} , "\n",
				             " START " , $iobj->{STARTWITH} , " CACHE " , $iobj->{CACHE} , "\n", ' ',
				             $iobj->{CYCLE}?'':'NO' , " CYCLE ",
							 "OWNED BY ", protect_reserved_keywords($schema),".",protect_reserved_keywords($table),".",protect_reserved_keywords($col), ";\n";
				print BEFORE "ALTER TABLE " , protect_reserved_keywords($schema),".", protect_reserved_keywords($table),
				             " ALTER COLUMN ",protect_reserved_keywords($col)," SET DEFAULT nextval('" ,protect_reserved_keywords($schema),".",
				             protect_reserved_keywords($seqname),"');\n";
				print AFTER "SELECT setval('\"" , protect_reserved_keywords($schema),"\".\"", protect_reserved_keywords($seqname),
							"\"',(SELECT max(" , protect_reserved_keywords($col) , ")::bigint FROM ", protect_reserved_keywords($schema),".", protect_reserved_keywords($table),"));\n";
			}
		}
	}


	


	
	
	# CHECK constraints
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		foreach my $table (keys(%{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}}))
		{
			my $tobj=$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table};
			
			foreach my $constraint (@{$tobj->{CONSTRAINTS}})
			{
				next unless ($constraint->{TYPE} eq 'CHECK');
				print UNSURE "ALTER TABLE ",protect_reserved_keywords($schema),".",protect_reserved_keywords($table), " ADD";
				if (exists $constraint->{NAME})
				{
					print UNSURE " CONSTRAINT " . protect_reserved_keywords($constraint->{NAME});
				}
				print UNSURE " CHECK ",$constraint->{CODE},";\n";
			}
		}
	}
	# Views
	foreach my $vobj (@{$schema_db2->{VIEWS}})
	{

		# Try to set the environment for the create view (these are different for DB2 and PG
		# FIXME: for now, hope that CURRENT_PATH is like PG's search_path. Ignore CURRENT_SCHEMA
		my $cleanedup_path=lc($vobj->{CURRENT_PATH});
		$cleanedup_path=~ s/"//g;
		print UNSURE "set search_path TO ",$cleanedup_path,",db2;\n";
		print UNSURE "CREATE VIEW " . protect_reserved_keywords($vobj->{SCHEMAS}),".",protect_reserved_keywords($vobj->{NAME})," ";
		# We try to fix what we can
		print UNSURE try_fix_expression($vobj->{STATEMENT}),";\n";
	}
	# Don't forget the comments on views. As it was easier, they aren't store in the same place (they are in the schema, not in a list)
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		foreach my $view (keys(%{$schema_db2->{SCHEMAS}->{$schema}->{VIEWS}}))
		{
			my $vobj=$schema_db2->{SCHEMAS}->{$schema}->{VIEWS}->{$view};
		
			if (exists $vobj->{COMMENT})
			{
				print UNSURE "COMMENT ON VIEW ",protect_reserved_keywords($schema),".",
								protect_reserved_keywords($view)," IS '",$vobj->{COMMENT},"';\n";
			}
		}
	}

	# Functions
	print UNSURE "-- Under this point, are functions. There is NO WAY they will work\n";
	print UNSURE "-- They are only here so that they crash at creation and you notice them and correct them by hand\n";
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		
		foreach my $function (keys %{$schema_db2->{SCHEMAS}->{$schema}->{FUNCTIONS}})
		{
			my $funcobj=$schema_db2->{SCHEMAS}->{$schema}->{FUNCTIONS}->{$function};
			print UNSURE "CREATE FUNCTION ", protect_reserved_keywords($function),
							" AS\n\$func\$\n",
							$funcobj->{STATEMENT},"\n\$func\$\n;\n";
		}
	}	

	# Triggers
	print UNSURE "-- Under this point, are triggers. There is NO WAY they will work\n";
	print UNSURE "-- They are only here so that they crash at creation and you notice them and correct them by hand\n";
	foreach my $schema (keys(%{$schema_db2->{SCHEMAS}}))
	{
		
		foreach my $trigger (keys %{$schema_db2->{SCHEMAS}->{$schema}->{TRIGGERS}})
		{
			my $trigobj=$schema_db2->{SCHEMAS}->{$schema}->{TRIGGERS}->{$trigger};
			print UNSURE "CREATE FUNCTION ", protect_reserved_keywords($trigger . '_fn'),
							" LANGUAGE plpgsql RETURNS (trigger) AS\n\$func\$\n",
							$trigobj->{STATEMENT},"\n\$func\$\n;\n";
			#FIXME: Should create the trigger, but that's not possible, as the function will fail
			print UNSURE "-- Add the CREATE TRIGGER too!\n\n\n";
		}
	}	
	
	close BEFORE;
	close AFTER;
	close UNSURE;
	
}


sub export_data
{
	# FIXME: for the moment, note all parameters to pass
	#codepage=UTF-8
	#modified by timestampformat="YYYY-MM-DD HH.MM.SS.uuuuuu"
	
	
	# Store what we need from the catalog, for deltocopy: a list of tables, colnames and types. Make a very simple, tab separated format
	my $tabledescname=$data_directory . '/' . "TABLEDESC";
	open TABLEDESC,">", $tabledescname or die "Cannot open $tabledescname for writing, $!";
	foreach my $schema(sort(keys %{$schema_db2->{SCHEMAS}}))
	{
		foreach my $table(sort(keys %{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}}))
		{
			my $tobj=$schema_db2->{SCHEMAS}->{$schema}->{TABLES}->{$table};
			print TABLEDESC "$schema\t$table\n";
			foreach my $col(sort { $tobj->{COLS}->{$a}->{COLNUM} <=> $tobj->{COLS}->{$b}->{COLNUM} } keys(%{$tobj->{COLS}}))
			{
				print TABLEDESC "\t$col\t",$tobj->{COLS}->{$col}->{ORIGTYPE},"\t", $tobj->{COLS}->{$col}->{NOTNULL}?'NOTNULL':'NULL', "\n";
			}
			#FIXME: add the command to dump the tables
		}
	}
	close TABLEDESC;
	
	# Produce the script
#	my $scriptname=$data_directory . '/export.db2' . (is_target_windows()?'.bat':'.sh');
	my $scriptname=$data_directory . '/export.db2';
	
	open SCRIPT,">",$scriptname or die "Cannot open $scriptname for writing, $!";


	
	# First connect
	print SCRIPT "connect to $db2dbname user $db2username using '$db2password'\n";
	# We sort, it gives a very rough idea of the export progression
	foreach my $schema(sort(keys %{$schema_db2->{SCHEMAS}}))
	{	foreach my $table(sort(keys %{$schema_db2->{SCHEMAS}->{$schema}->{TABLES}}))
		{
			print SCRIPT "EXPORT TO ${schema}.${table}.del  of del  LOBS to . MODIFIED BY LOBSINFILE messages ${schema}.${table}.log  SELECT  * FROM \"${schema}\".\"${table}\"\n";
		}
	}
	close SCRIPT;
}

# To know if the operating system is windows or linux
sub is_os_windows
{
	
    if ($^O =~ /MSWin32/)
    {
        return 1;
    }
    return 0;
}

# To know if we want to produce a unix or windows script
sub is_target_windows
{
	return 0 if ($data_script_type eq 'unix');
	return 1;
}


sub usage
{
	print STDERR "$0:\n",
	"  -f db2's sql dump file\n",
	"  -o output directory\n",
	"  -d DB2's database name\n",
	"  -u DB2's user name\n",
	"  -p DB2's password\n",
	"  [-tbs] produce the create tablespace statements. Probably not a good idea\n",
	"  [-script unix|windows] force the script generation to be .sh or .bat. Autodetected if not\n",
	"  -h this help\n"
	or die "FAIL";
}

# MAIN

# Read all the command line parameters

my $options = GetOptions("f=s"	  => \$filename,
						 "tbs"	  => \$do_tablespaces,
						 "o=s"	  => \$data_directory,
						 "d=s"	  => \$db2dbname,
						 "u=s"	  => \$db2username,
						 "p=s"	  => \$db2password,
                         "h"      => \$help,
						 "script=s"	  => \$data_script_type,
                         );

# We don't understand command line or have been asked for usage
if (not $options or $help)
{
    usage();
    exit 1;
}

if (defined $data_script_type)
{
	usage() unless ($data_script_type =~ '^(unix|windows)$');
}
else
{
	if (is_os_windows())
	{
		$data_script_type='windows';
	}
	else
	{
		$data_script_type='unix';
	}
}

# We have no before, after, or unsure, or no filename to parse
if ( not $filename or not $data_directory)
{
    usage();
    exit 1;
}

	






parse_dump($filename);

$data_directory =~ s/(\/+|\\+)$//; # Remove the trailing / and \

# First thing: check the path is an existing directory or create it
unless (-d $data_directory)
{
	mkdir($data_directory) or die "Cannot create $data_directory, $!";
}


produce_schema_files();
if (defined $data_directory)
{
	export_data();
}
open DMP,'>','/tmp/debug';
print DMP Dumper($schema_db2);
close DMP;
