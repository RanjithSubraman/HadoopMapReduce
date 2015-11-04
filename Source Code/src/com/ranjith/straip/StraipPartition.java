package com.ranjith.straip;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Partitioner;

public class StraipPartition extends MapReduceBase implements
		Partitioner<Text, MapWritable> {

	@Override
	public int getPartition(Text key, MapWritable values, int partitions) {
		return key.hashCode() % partitions;
	}

}
