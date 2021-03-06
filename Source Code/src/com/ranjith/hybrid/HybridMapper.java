package com.ranjith.hybrid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ranjith.pair.Pair;

public class HybridMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Pair, IntWritable> {

	private HashMap<Pair, Integer> outputMap = new HashMap<>();

	@Override
	public void map(LongWritable key, Text values,
			OutputCollector<Pair, IntWritable> output, Reporter r)
			throws IOException {
		String wholeInput = values.toString();
		String[] readLines = wholeInput.split("//.*\n");
		for (String line : readLines) {
			String[] words = line.split(" ");

			for (int i = 0; i < words.length - 1; i++) {
				for (int j = i + 1; j < words.length; j++) {
					if (words[i] == words[j])
						break;

					Pair pair = new Pair(words[i], words[j]);
					if (outputMap.get(pair) != null) {
						outputMap.put(pair, outputMap.get(pair) + 1);
					} else {
						outputMap.put(pair, 1);
					}

				}
			}
		}

		Iterator<Entry<Pair, Integer>> iterator = outputMap.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			Map.Entry<Pair, Integer> entry = (Map.Entry<Pair, Integer>) iterator
					.next();
			output.collect(entry.getKey(), new IntWritable(entry.getValue()));
		}
	}
}
