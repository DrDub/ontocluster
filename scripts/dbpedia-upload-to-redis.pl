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
$redis->flushdb; # bye bye data

# hash res-Alfred_Nobel__Chemist%2C_engineer%2C_innovator%2C_armaments_manufactur
# fields: one per entry (e.g., knownFor), value is count

# list knownFor-Alfred_Nobel__Chemist%2C_engineer%2C_innovator%2C_armaments_manufactur
# values: each of the values inst- if actual instance, otherwise string

# keys: subj, obj, verb-type, verb
my$script=<<'LUA';
    local function check_create(name)
      local key = redis.call("get", name)
      if not key then
        local len = redis.call("dbsize")
        key = string.format("%d", len)
        redis.call("set", name, key)
      end -- if
      return key
    end -- function

    local subj_key = check_create(string.format("res-%s", KEYS[1]))
    local obj_key_or_value
    if KEYS[3] == "res" then
      obj_key_or_value = string.format("r-%s", check_create(string.format("res-%s", KEYS[4])))
    elseif KEYS[3] == "0" then
      obj_key_or_value = KEYS[4]
    else
      obj_key_or_value = string.format("%s-%s", KEYS[3], KEYS[4])
    end -- if

    local lkey = string.format("%s-%s", KEYS[2], subj_key)
    redis.call("lpush", lkey, obj_key_or_value)
    redis.call("sadd", string.format("r-%s", subj_key), KEYS[2])
    return subj_key
LUA

# first do types

my$count=0;
my$CHUNK_SIZE=20000;

open(T,"bzcat /path/to/dbpedia/instance_types_en.nt.bz2|")or die"$!\n";
#<http://dbpedia.org/resource/Amphibian> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Thing> .
$redis->multi;
while(<T>){
    chomp;
    s/\s\.$//;
    my($subj,$verb,$obj)=split(/\s/,$_);
    unless($subj =~ m/\<http:\/\/dbpedia.org\/resource\//){
	die "Unknown subj: '$subj'\n";
    }
    $subj =~ s/\<http:\/\/dbpedia.org\/resource\///;
    $subj =~ s/\>$//;
    unless($verb eq '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'){
	die "Unknown verb: '$verb'\n";
    }
    my$obj_type;
    if($obj =~ m/\<http:\/\/dbpedia.org\/resource\//){
	$obj =~ s/\<http:\/\/dbpedia.org\/resource\///;
	$obj =~ s/\>$//;
	$obj_type = "res";
    }elsif($obj =~ m/\<http:\/\/dbpedia.org\/ontology\//){
	    $obj =~ s/\<http:\/\/dbpedia.org\/ontology\///;
	    $obj =~ s/\>$//;
	    $obj_type = "onto";
    }elsif($obj eq '<http://www.w3.org/2002/07/owl#Thing>'){
	# ignore
	next;
    }else{
	die "Unknown obj: '$obj'\n";
    }
    #print "$subj type $obj\n";
    $redis->eval($script, 4, $subj, 'type', $obj_type, $obj);
    #$redis->lpush("type-$subj", $obj);
    #$redis->hset("res-$subj", "type", $redis->llen("type-$subj"));
    $count++;
    if($count==$CHUNK_SIZE){
	$redis->exec;
	$count=0;
	$redis->multi;
    }
}
close T;
if($count){
    $redis->exec;
}

print "Done with types\n";

# now for the properties

open(P,"bzcat /path/to/dbpedia/mappingbased_properties_en.nt.bz2|")or die"$!\n";
# <http://dbpedia.org/resource/Alain_Connes> <http://dbpedia.org/ontology/knownFor> <http://dbpedia.org/resource/Baum%E2%80%93Connes_conjecture> .
$count=0;
$redis->multi;
while(<P>){
    chomp;
    s/\r//;
    s/\s\.$//;
    my($subj,$verb,$obj)=m/^(\<[^>]+>) (\<[^>]+>) (.*)$/;
    unless($subj =~ m/\<http:\/\/dbpedia.org\/resource\//){
	die "Unknown subj: '$subj'\n";
    }
    $subj =~ s/\<http:\/\/dbpedia.org\/resource\///;
    $subj =~ s/\>$//;
    if($verb =~ m/\<http:\/\/dbpedia.org\/ontology\//){
	$verb =~ s/\<http:\/\/dbpedia.org\/ontology\///;
	$verb =~ s/\>$//;
    }elsif($verb eq '<http://xmlns.com/foaf/0.1/name>'){
	$verb = 'name';
    }elsif($verb eq '<http://www.georss.org/georss/point>'){
	$verb = 'point';
    }elsif($verb eq '<http://www.w3.org/2003/01/geo/wgs84_pos#long>'){
	$verb = 'wgs84_pos_long';
    }elsif($verb eq '<http://www.w3.org/2003/01/geo/wgs84_pos#lat>'){
	$verb = 'wgs84_pos_lat';
    }elsif($verb eq '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'){
	$verb = 'type';
    }elsif($verb =~ m/\<http:\/\/xmlns.com\/foaf\/0.1\//){
	($verb) = $verb =~ m/\<http:\/\/xmlns.com\/foaf\/0.1\/(.*)\>/;
	$verb = "foaf_$verb";
    }else{
	die "Unknown verb: '$verb'\n";
    }
    my$obj_type=0;
    if($obj =~ m/\<http:\/\/dbpedia.org\/resource\//){
	$obj =~ s/\<http:\/\/dbpedia.org\/resource\///;
	$obj =~ s/\>$//;
	$obj_type = "res";
    }elsif($obj =~ m/\<http:\/\/dbpedia.org\/ontology\//){
	    $obj =~ s/\<http:\/\/dbpedia.org\/ontology\///;
	    $obj =~ s/\>$//;
	    $obj_type = "onto";
    }elsif($obj =~ m/\@en/){
	($obj) = $obj=~ m/(\".+\")\@en/;
	#$obj_type="str";
    }elsif($obj =~ m/\^\^\<http:\/\/www.w3.org\/2001\/XMLSchema\#((date)|(boolean)|(anyURI))\>/){
	($obj) = $obj=~ m/(\"[^"]+\")\^\^/;
	#$obj_type="str";
    }elsif($obj =~ m/\^\^\<http:\/\/www.w3.org\/2001\/XMLSchema\#((((positive)|(nonNegative))?[iI]nteger)|(gYear)|(double)|(float))\>/){
	($obj) = $obj=~ m/\"([^"]+)\"\^\^/;
	#$obj_type="num";
    }elsif($obj =~ m/\^\^\<http:\/\/dbpedia.org\/datatype\//){
	($obj) = $obj=~ m/(\"[^"]+\")\^\^/;
	#$obj_type="str";
    }elsif($obj eq '<http://www.opengis.net/gml/_Feature>'){
	$obj = 'gis_feature';
	#$obj_type="feat";
    }else{
	if($verb eq 'foaf_homepage'){
	    $obj=~s/\<//;
	    $obj=~s/\>//;
	    $obj="\"$obj\"";
	    #$obj_type="str";
	}else{
	    die "Unknown obj: '".$obj."'\n";
	}
    }
    $redis->eval($script,4, $subj, $verb, $obj_type, $obj);

    $count++;
    if($count==$CHUNK_SIZE){
	$redis->exec;
	$count=0;
	$redis->multi;
    }
}
close P;

if($count){
    $redis->exec;
}
$redis->quit;
