OntoCluster
===========

Cluster dbPedia (http://dbpedia.org) instances based on the type of relations defined for them.

Case study for http://aprendizajengrande.net


Getting the data
----------------

You'll need redis version 2.8.17 or later (see below).

Full dump (with values per instance): http://aprendizajengrande.net/dbpedia_full.rdb.7z (182Mb, ~16M keys)
Clean dump (for use with current code): http://aprendizajengrande.net/dbpedia.rdb.7z (65Mb, ~6M keys)

To use, stop redis (service redis-server stop), uncompress the file using 7zip and put its 
contents with name dump.rdb in /var/lib/redis, owned by user redis, group redis. 
Then restart redis.


Generating the data
-------------------

Get instance_types_en.nt.bz2 and mappingbased_properties_en.nt.bz2.

Set the path in scripts/dbpedia-upload-to-redis.pl

Execute 

$ perl dbpedia-upload-to-redis.pl
$ perl clean-redis.pl


Redis Version
-------------

You'll need version 2.8.17 or later:

deb http://http.debian.net/debian/ wheezy-backports main contrib non-free
deb-src http://http.debian.net/debian/ wheezy-backports main contrib non-free

apt-get install -t wheezy-backports redis-server