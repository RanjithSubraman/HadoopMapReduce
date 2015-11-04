package com.ranjith.pair;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Partitioner;

public class PairPartition extends MapReduceBase implements
		Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair key, IntWritable value, int partitions) {
		return key.getFirstElement().hashCode() % partitions;
	}

}
