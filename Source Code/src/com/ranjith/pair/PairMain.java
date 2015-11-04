package com.ranjith.pair;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PairMain extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("plz give and output directory pefectly");
		}
		JobConf conf = new JobConf(PairMain.class);

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// set the Mapper, Partitioner and Reducer classes to JobConf
		conf.setMapperClass(PairMapper.class);
		conf.setPartitionerClass(PairPartition.class);
		conf.setReducerClass(PairReducer.class);

		// set the Mapper output key and value classes
		conf.setMapOutputKeyClass(Pair.class);
		conf.setMapOutputValueClass(IntWritable.class);

		// set the Reducer output key and value classes
		conf.setOutputKeyClass(Pair.class);
		conf.setOutputValueClass(IntWritable.class);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new PairMain(), args);
		System.exit(exitCode);
	}

}
