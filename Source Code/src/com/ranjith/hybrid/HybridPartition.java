package com.ranjith.hybrid;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Partitioner;

import com.ranjith.pair.Pair;

public class HybridPartition extends MapReduceBase implements
		Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair key, IntWritable value, int partitions) {
		return key.getFirstElement().hashCode() % partitions;
	}

}
