#!/bin/perl
use strict;
use warnings;
use Time::Local;
use DBI;

my $workdir = "/root/maillog";
my $file_maillog = "maillog";
my $logfile = "$workdir/$0.log";
my $logfile_fl = 0;

my $db_driver = "Pg"; 
my $db_host = "192.168.6.100";
my $db_port = "5432";
my $db_name = 'maildb';
my $db_pwd = "12345678";
my $db_user = "mailuser";
my @flags = ("\<\=","\=\>","\=\=","\*\*","\-\>");

#{AutoCommit=>1,RaiseError=>1,PrintError=>0}

my $dsn = "DBI:$db_driver:dbname=$db_name;host=$db_host;port=$db_port";
my $dbh = DBI->connect($dsn, $db_user, $db_pwd, { RaiseError => 1 }) 
   or die $DBI::errstr;
my $db_test=$dbh->selectrow_array("SELECT 2+2");
    if ( $db_test == 4) {print "Connected to database\n";} else {die;};

read_log();

$dbh->disconnect();

exit 0;

#sub
sub read_log {
    open(FILE,"$file_maillog") or die;
    while(<FILE>) {
        next if (/^#/ || /^$/);

    #    $_ =~ /(\d+-\d+-\d+\s\d+:\d+:\d+)\s(\S+)\s(.*)/;
    #    $_ =~ /(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\s(\S+)\s([\>|\<|\=|\-|\*]{2})\s(.*\@.*?)\s(.*)/;

        $_ =~ /^(\d+-\d+-\d+\s\d+:\d+:\d+)\s(\S+)\s(.*)/;
        my $str_full =$_;
        my $date = $1;
        my $int_id = $2;
        my $str = $3;
        my $flag = 0;
        my $flag_in = '<=';
        my $table = "log";
        my $id = 0;
        my $address = 0;
#        my ($year,$mon,$mday,,$hour,$min,$sec) = split(/[\s\-\:]+/, $date);
#        my $created = timelocal($sec,$min,$hour,$mday,$mon-1,$year);
        my $created = $date;
        $str =~ /^([\>|\<|\=|\-|\*]{2}).*/;
        $flag = $1;
        if (  ($flag =~ /\<\=/) or ($flag =~ /\=\>/) or ($flag =~ /\-\>/) or ($flag =~ /\*\*/) or ($flag =~ /\=\=/) )  {
                if ($flag eq $flag_in) {
                    $str =~ /.*id=(.*)/g;
                    if (defined $1 ) {
                        $id = $1;
                        $table = "message";
                    } else {
                        $table = "log";
                    }
                }

                $str =~ /^([\>|\<|\=|\-|\*]{2}).*\s(.*\@.*?)\s(.*)/;
                if (defined $2) {
                    $address = $2;
                    $address =~ s/TO://;
                    $address =~ s/[<|>|:]//ig;
                }
            }
#        }
#        print_sql($table,$created,$flag,$id,$int_id,$str,$address,$str_full);
        insert2db($table,$created,$flag,$id,$int_id,$str,$address);
    }
    close(FILE);
}

sub print_sql {
    my $table = $_[0];
    my $created = $_[1];
    my $flag = $_[2];
    my $id = $_[3];
    my $int_id = $_[4];
    my $str = $_[5];
    my $address = $_[6];
    my $str_full = $_[7];
    my $status = "0";
    my $msg = "TABLE:$table
    TIMESTAMP:$created
    FLAG:$flag
    ID:$id
    INT_ID:$int_id
    STR:$str
    ADDRESS:$address
    STATUS:$status
    STR_FULL:$str_full\n";
    log_write_replace($msg);
}

sub insert2db {
    my $table = $_[0];
    my $created = $_[1];
    my $flag = $_[2];
    my $id = $_[3];
    my $int_id = $_[4];
    my $str = $_[5];
    my $address = $_[6];
#    my $str_full = $_[7];
    my $status = "0";
    if ($table eq "log") {
        my $res = $dbh->prepare("INSERT INTO $table (created,int_id,str,address) VALUES (?, ?, ?, ?)");
        $res->execute($created,$int_id,$str,$address) or die $DBI::errstr;
    }
    if ($table eq "message") {
        my $res = $dbh->prepare("INSERT INTO $table (created,id,int_id,str,status) VALUES (?, ?, ?, ?, ?)");
        $res->execute($created,$id,$int_id,$str,$status) or die $DBI::errstr;
    }
}


#sub system
sub log_write_add {
    my $text=$_[0];
    open(LOGS,">> $logfile") or die ("Cannot open file Log List File or File isn\'t writable: $logfile");
    my $ts=timestamp();
    $text="$ts\t$text\n";
    print LOGS $text;  print $text;
    close LOGS;
}

sub log_write_replace {
    my $text=$_[0];
    my $lwr = ">>";
    if ($logfile_fl == 0) {$lwr = ">";$logfile_fl++;}
    open(LOGS,"$lwr $logfile") or die ("Cannot open file Log List File or File isn\'t writable: $logfile");
    my $ts=timestamp();
    $text="$ts\t$text\n";
    print LOGS $text;  print $text;
    close LOGS;
}

sub zero_add {
    my $dig=$_[0];
    if ($dig < 10) {return "0$dig";} else {return $dig;}
}

sub timestamp {
    my $arg=$_[0];
    my $result='';
    if (defined $arg != 1) {$arg="full";}
    my $sec=''; my $min=''; my $hours=''; my $day=''; my $month=''; my $year=''; 
    ($sec,$min,$hours,$day,$month,$year)=(localtime)[0,1,2,3,4,5];
    $year=1900+$year;
    $month++;
    $month=zero_add($month);
    $day=zero_add($day);
    $hours=zero_add($hours);
    $min=zero_add($min);
    $sec=zero_add($sec);
    if ($arg eq "d") {$result=$day; return $result;e xit;}
    if ($arg eq "ymd") {$result="$year$month$day"; return $result; exit;}
    if ($arg eq "ym") {$result="$year$month"; return $result; exit;}
    if ($arg eq "fdate") {$result="$year$month$day\_$hours$min$sec"; return $result; exit;} 
    $result="$year\.$month\.$day $hours\:$min\:$sec"; return $result;
}
