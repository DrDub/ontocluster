/*
 *   This file is part of ontocluster
 *   Copyright (C) 2014 Pablo Duboue <pablo.duboue@gmail.com>
 * 
 *   ontocluster is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as 
 *   published by the Free Software Foundation, either version 3 of 
 *   the License, or (at your option) any later version.
 *
 *   ontocluster is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *   
 *   You should have received a copy of the GNU General Public License 
 *   along with ontocluster.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.aprendizajengrande.ontocluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;

public class ClusterExtractor {

	public static Path findFinalClusters(String prefix, Configuration conf)
			throws IllegalArgumentException, IOException {
		int numIterations = 0;
		Path result = new Path(prefix + "/clusters-" + numIterations + "-final");
		while (!result.getFileSystem(conf).exists(result)
				&& numIterations < 20000) {
			numIterations++;
			result = new Path(prefix + "/clusters-" + numIterations + "-final");
		}
		if (numIterations == 20000) {
			return null;
		}
		return result;
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		if (args.length != 3) {
			System.err
					.println("Usage: <input hdfs folder with rels> <hdfs folder for output> <local folder for output>");
			System.exit(1);
		}

		Configuration conf = new Configuration();

		// see
		// http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());

		// crear vectores en HDFS
		System.out.println("Input: " + args[0]);

		// read the rel names, to pretty print

		Path inputRels = new Path(args[0] + "/rels");
		FileSystem fs = inputRels.getFileSystem(conf);
		FSDataInputStream fsdis = fs.open(inputRels);
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));
		String line = br.readLine();
		Map<Integer, String> relIdToName = new HashMap<>();
		while (line != null) {
			String[] parts = line.split("\\t");
			relIdToName.put(Integer.parseInt(parts[0]), parts[1]);
			line = br.readLine();
		}

		// read output
		Path outputFinal = findFinalClusters(args[1], conf);
		if (outputFinal == null) {
			System.err.println("Couldn't find final clusters at '" + args[1]
					+ "-\\d+-final'");
			System.exit(1);
		}

		// delete the _SUCCESS file as it is problematic
		// see
		// http://stackoverflow.com/questions/10752708/eofexception-at-org-apache-hadoop-io-sequencefilereader-initsequencefile-java
		Path successFile = new Path(outputFinal, "_SUCCESS");
		if (fs.exists(successFile)) {
			fs.delete(successFile, false);
		}

		SequenceFileDirIterable<Text, Writable> it = new SequenceFileDirIterable<>(
				outputFinal, PathType.LIST, conf);

		PrintWriter pw = new PrintWriter(new FileWriter(new File(args[2])));

		int clusterNum = 0;
		for (Pair<Text, Writable> p : it) {
			Object obj = p.getSecond();
			if (!(obj instanceof ClusterWritable))
				continue;
			pw.println(clusterNum + ") " + p.getFirst());
			Cluster cluster = ((ClusterWritable) obj).getValue();
			Vector center = cluster.getCenter();
			for (int i = 0; i < center.size(); i++) {
				String name = relIdToName.get(i);
				if (name == null)
					name = "?";
				if (center.get(i) >= 0.01)
					pw.println("\t" + name + ": " + center.get(i));
			}
			pw.println();
			clusterNum++;
		}
		pw.close();
	}
}
