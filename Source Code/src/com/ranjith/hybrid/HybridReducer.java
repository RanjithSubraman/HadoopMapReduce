package com.ranjith.hybrid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.ranjith.pair.Pair;

public class HybridReducer extends MapReduceBase implements
		Reducer<Pair, IntWritable, Text, Text> {
	private int total = 0;
	private Text prevKey;
	private Map<String, Integer> straip = new HashMap<>();

	@Override
	public void reduce(Pair key, Iterator<IntWritable> values,
			OutputCollector<Text, Text> output, Reporter r) throws IOException {
		Text leftKey = key.getFirstElement();
		Text rightKey = key.getSecondElement();

		if (prevKey != null && !prevKey.equals(leftKey)) {
			printOutput(prevKey, output);
			total = 0;
			prevKey = leftKey;
			straip = new HashMap<String, Integer>();
		}

		while (values.hasNext()) {
			IntWritable i = values.next();

			Integer rightKeyOldValue = straip.get(rightKey);
			if (rightKeyOldValue != null) {
				straip.put(rightKey.toString(), rightKeyOldValue + i.get());
			} else {
				straip.put(rightKey.toString(), i.get());
			}

			total += i.get();
		}
		prevKey = leftKey;
	}

	private void printOutput(Text prevKey, OutputCollector<Text, Text> output)
			throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("[");

		Iterator<Entry<String, Integer>> iterator = straip.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			MapWritable.Entry<String, Integer> entry = (MapWritable.Entry<String, Integer>) iterator
					.next();
			sb.append("(");
			sb.append(entry.getKey().toString());
			sb.append(", ");
			sb.append((entry.getValue()) / (double) total);
			sb.append(")");
		}

		sb.append("]");
		output.collect(prevKey, new Text(sb.toString()));
	}
}
