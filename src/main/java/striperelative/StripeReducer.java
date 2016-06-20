package striperelative;


import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReducer extends
		Reducer<Text, MapWritable, Text, StripeWritable> {

	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		StripeWritable map = new StripeWritable();
		int total = 0;
		for (MapWritable mapWritable : values) {
			for (Entry<Writable, Writable> e : mapWritable.entrySet()) {
				if (!map.containsKey(e.getKey())) {
					map.put(e.getKey(), new IntWritable(0));
				}
				IntWritable in = (IntWritable) map.get(e.getKey());
				int k = ((IntWritable) (e.getValue())).get();
				in.set(in.get() + k);
				total += k;
			}
		}
		for (Entry<Writable, Writable> e : map.entrySet()) {
			IntWritable intw = (IntWritable) e.getValue();
			Text t = new Text(intw + "/" + total);
			map.put(e.getKey(), t);
		}
		context.write(key, map);

	}
}