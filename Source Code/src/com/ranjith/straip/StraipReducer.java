package com.ranjith.straip;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StraipReducer extends MapReduceBase implements
		Reducer<Text, MapWritable, Text, Text> {

	MapWritable mapWritable = new MapWritable();
	int total = 0;

	@Override
	public void reduce(Text key, Iterator<MapWritable> values,
			OutputCollector<Text, Text> output, Reporter r) throws IOException {
		while (values.hasNext()) {

			MapWritable i = values.next();
			Iterator<Entry<Writable, Writable>> iterator = i.entrySet()
					.iterator();
			while (iterator.hasNext()) {

				MapWritable.Entry<Writable, Writable> entry = (MapWritable.Entry<Writable, Writable>) iterator
						.next();
				Text text = (Text) entry.getKey();
				IntWritable in = (IntWritable) entry.getValue();

				if (mapWritable.get(text) != null) {
					IntWritable count = (IntWritable) mapWritable.get(text);
					mapWritable.put(text,
							new IntWritable(count.get() + in.get()));
				} else {
					mapWritable.put(text, in);
				}

				total += in.get();
			}
		}

		StringBuilder sb = new StringBuilder();
		sb.append("[");

		Iterator<Entry<Writable, Writable>> iterator = mapWritable.entrySet()
				.iterator();
		while (iterator.hasNext()) {
			MapWritable.Entry<Writable, Writable> entry = (MapWritable.Entry<Writable, Writable>) iterator
					.next();
			sb.append("(");
			sb.append(entry.getKey().toString());
			sb.append(", ");
			sb.append(((IntWritable) entry.getValue()).get() / (double) total);
			sb.append(")");
		}

		sb.append("]");
		output.collect(key, new Text(sb.toString()));
	}

}
