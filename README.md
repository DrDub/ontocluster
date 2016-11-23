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

(You don't need these steps if you use the redis dump above.)

Get instance_types_en.nt.bz2 and mappingbased_properties_en.nt.bz2.

Set the path in scripts/dbpedia-upload-to-redis.pl

Execute 

```
$ perl dbpedia-upload-to-redis.pl
$ perl clean-redis.pl
```

Redis Version
-------------

You'll need version 2.8.17 or later:

deb http://http.debian.net/debian/ wheezy-backports main contrib non-free
deb-src http://http.debian.net/debian/ wheezy-backports main contrib non-free

```
apt-get install -t wheezy-backports redis-server
```

Sample Output
-------------

See http://aprendizajengrande.net/clases/material/cluster_output_dbpedia_250_0.005_cosine.txt

Running the code
----------------

*you need to have Mahout 1.0 installed from source in your local repo,
configured for Hadoop 2.0, see below*

```
mvn clean package assembly:single
```

Then run the hadoop job org.keywords4bytecodes.firstclass.Driver
pointing to the tsv file training file (see below) and an output
directory.

Installing Mahout from source
-----------------------------

```
$ git clone https://github.com/apache/mahout.git
$ cd mahout
$ mvn clean package -DskipTests -Drelease -Dmahout.skip.distribution=false -Dhadoop.profile=200 -Dhadoop2.version=2.4.1 -Dhbase.version=0.98.0-hadoop2
```
