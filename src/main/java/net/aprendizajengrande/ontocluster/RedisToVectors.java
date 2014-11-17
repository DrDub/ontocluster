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

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import redis.clients.jedis.Jedis;

public class RedisToVectors {

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.println("Usage: <hdfs folder for input>");
			System.exit(1);
		}

		Configuration conf = new Configuration();

		String inputName = args[0] + "/input";
		String relsInputName = args[0] + "/rels";
		String instancesInputName = args[0] + "/instances";

		Path input = new Path(inputName);
		Path relsInput = new Path(relsInputName);
		Path instancesInput = new Path(instancesInputName);

		Jedis jedis = new Jedis("localhost");

		// create the relations and instances first, so we know what to expect
		jedis.getClient().keys("rel-nom-*");
		List<String> rels = jedis.getClient().getMultiBulkReply();

		Map<Integer, String> relIdToName = new HashMap<>();

		FSDataOutputStream fsdos = relsInput.getFileSystem(conf).create(
				relsInput);
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(fsdos));

		int relNum = 0;
		for (String rel : rels) {
			String relName = rel.replaceAll("^rel-nom-", "");
			int relId = Integer.parseInt(jedis.get(rel));
			relIdToName.put(relId, relName);
			if (relId > relNum)
				relNum = relId;
		}
		relNum++;
		for (int i = 0; i < relNum; i++)
			pw.println(i + "\t" + relIdToName.get(i));
		pw.close();
		rels.clear();

		jedis.getClient().keys("res-nom-*");
		List<String> instances = jedis.getClient().getMultiBulkReply();

		fsdos = instancesInput.getFileSystem(conf).create(instancesInput);
		pw = new PrintWriter(new OutputStreamWriter(fsdos));

		for (String instance : instances) {
			int instanceId = Integer.parseInt(instance.replaceAll("^res-nom-",
					""));
			String instanceName = jedis.get(instance);
			pw.println(instanceId + "\t" + instanceName);
		}
		pw.close();
		instances.clear();

		jedis.getClient().keys("r-*");
		List<String> keys = jedis.getClient().getMultiBulkReply();

		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				Writer.file(input), Writer.keyClass(Text.class),
				Writer.valueClass(VectorWritable.class));

		for (String key : keys) {
			Set<String> theseRels = jedis.smembers(key);

			Vector s = new SequentialAccessSparseVector(relNum);
			for (String relId : theseRels)
				s.set(Integer.parseInt(relId), 1.0);
			VectorWritable v = new VectorWritable(s);
			writer.append(new Text(key), v);
		}
		writer.close();
		
		jedis.close();	
	}
}
