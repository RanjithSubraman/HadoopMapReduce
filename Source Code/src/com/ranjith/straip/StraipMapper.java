package com.ranjith.straip;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StraipMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, MapWritable> {

	@Override
	public void map(LongWritable id, Text values,
			OutputCollector<Text, MapWritable> output, Reporter r)
			throws IOException {
		String wholeInput = values.toString();
		String[] readLines = wholeInput.split("//.*\n");
		for (String line : readLines) {
			String[] words = line.split(" ");

			for (int i = 0; i < words.length - 1; i++) {
				Text key = new Text(words[i]);
				MapWritable mapWritable = new MapWritable();

				for (int j = i + 1; j < words.length; j++) {
					Text word = new Text(words[j]);
					if (key.equals(word))
						break;

					if (mapWritable.containsKey(word)) {
						IntWritable count = (IntWritable) mapWritable.get(word);
						mapWritable.put(word, new IntWritable(count.get() + 1));
					} else {
						mapWritable.put(word, new IntWritable(1));
					}
				}

				output.collect(key, mapWritable);
			}
		}

	}

}
