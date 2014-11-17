#!/usr/bin/perl -w

#  This file is part of ontocluster
#  Copyright (C) 2014 Pablo Duboue <pablo.duboue@gmail.com>
#
#  ontocluster is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as 
#  published by the Free Software Foundation, either version 3 of 
#  the License, or (at your option) any later version.
#
#  ontocluster is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License 
#  along with ontocluster.  If not, see <http://www.gnu.org/licenses/>.

use strict;
use Redis;

my$redis=Redis->new;

my@keys;
my$count;

if(0){
$redis->select(1);
$redis->flushdb;

$redis->select(0);
@keys=$redis->keys('r-*');
print "Got ".scalar(@keys)." keys\n";
$count=0;
$redis->multi;
foreach my$k(@keys){
    my$ok = $redis->move($k,1);
    die unless($ok);
    $count++;
    if($count==100000){
	$redis->exec;
	$count=0;
	$redis->multi;
    }
}
$redis->exec;
print "r-\n";

@keys=$redis->keys('res-*');
print "Got ".scalar(@keys)." keys\n";
$count=0;
$redis->multi;
foreach my$k(@keys){
    my$ok = $redis->move($k,1);
    die unless($ok);
    $count++;
    if($count==100000){
	$redis->exec;
	$count=0;
	$redis->multi;
    }
}
$redis->exec;
print "res-\n";
}
$redis->select(0);
$redis->flushdb;

$redis->select(1);
@keys=$redis->keys('r-*');
print "Got ".scalar(@keys)." keys\n";
$count=0;
my%keys = ();
foreach my$k(@keys){
    $redis->select(1);
    my@members = $redis->smembers($k);
    $redis->select(0);
    $redis->multi;
    foreach my$m(@members){
	if(!defined($keys{$m})){
	    my$val = scalar(keys %keys);
	    $keys{$m} = $val;
	    $redis->set("rel-$val", $m);
	    $redis->set("rel-nom-$m", $val);
	}
	$redis->sadd($k, $keys{$m});
    }
    $redis->exec;
    $count++;
    if($count%100000 == 0){
	print "$count\n";
    }
}
print "r-\n";

$redis->select(1);
@keys=$redis->keys('res-*');
print "Got ".scalar(@keys)." keys\n";
$count=0;
foreach my$k(@keys){
    $redis->select(1);
    my$target = $redis->get($k);
    $redis->select(0);
    $redis->set($k, $target);
    $target=~s/^r-//;
    $k=~s/^res-//;
    $redis->set("res-nom-$target", $k);
    $count++;
    if($count%100000 == 0){
	print "$count\n";
    }
}
print "res-\n";

$redis->select(1);
$redis->flushdb;


