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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;

public class Clusterer {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {

		if (args.length != 1) {
			System.err
					.println("Usage: <input hdfs folder with vectors> <hdfs folder for output> <local folder for output>");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		DistanceMeasure measure = new CosineDistanceMeasure();
		long seed = 67241;
		int numClusters = 250;
		int numIterations = 500;

		// crear vectores en HDFS
		System.out.println("Input: " + args[0]);
		Path input = new Path(args[0] + "/input");

		// first centroids are an input parameter to clustering
		Path clusters = new Path(args[0] + "/clusters");
		clusters = RandomSeedGenerator.buildRandom(conf, input, clusters,
				numClusters, measure, seed);

		Path output = new Path(args[1]);
		Path outputFinal = new Path(args[1] + "/clusters-" + numIterations
				+ "-final");

		// cluster
		KMeansDriver.run(input, clusters, output, 0.5, numIterations, true,
				0.0, false);

		// read the rel names, to pretty print

		Path inputRels = new Path(args[0] + "/rels");
		FSDataInputStream fsdis = inputRels.getFileSystem(conf).open(inputRels);
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));
		String line = br.readLine();
		Map<Integer, String> relIdToName = new HashMap<>();
		while (line != null) {
			String[] parts = line.split("\\t");
			relIdToName.put(Integer.parseInt(parts[0]), parts[1]);
			line = br.readLine();
		}

		// read output
		SequenceFileDirIterable<Text, ClusterWritable> it = new SequenceFileDirIterable<>(
				outputFinal, PathType.LIST, conf);

		PrintWriter pw = new PrintWriter(new FileWriter(new File(args[2])));

		int clusterNum = 0;
		for (Pair<Text, ClusterWritable> p : it) {
			pw.println(p.getFirst());
			Cluster cluster = p.getSecond().getValue();
			Vector center = cluster.getCenter();
			for (int i = 0; i < center.size(); i++) {
				String name = relIdToName.get(i);
				if (name == null)
					name = "?";
				pw.println("\t" + name + ": " + center.get(i));
			}
			pw.println();
			clusterNum++;
			if (clusterNum == numClusters)
				break;
		}
		pw.close();
	}
}
