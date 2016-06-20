package hybridrelative;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;


public class MapperHybrid extends
Mapper<Object, Text, Pair, IntWritable> {
	MapWritable inmap = new MapWritable();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		ArrayList<String> list = new ArrayList<String>();
		while (itr.hasMoreTokens()) {
			list.add(itr.nextToken());
		}

		for (int i = 0; i < list.size(); i++) {

			for (int j = i + 1; j < list.size(); j++) {
				Pair pair = new Pair();
				Pair pair1 = new Pair();
				pair.getPrimary().set(list.get(i));
				pair1.getPrimary().set(list.get(i));
				String secondary = list.get(j);
				if (pair.getPrimary().toString().equals(secondary)) {
					break;
				}
				pair.getSecondary().set(list.get(j));
				if (!inmap.containsKey(pair)) {
					inmap.put(pair, new IntWritable(0));
				}
				IntWritable ii = (IntWritable) inmap.get(pair);
				ii.set(ii.get() + 1);
				pair1.getSecondary().set("0");
				;
				if (!inmap.containsKey(pair1)) {
					inmap.put(pair1, new IntWritable(0));
				}
				ii = (IntWritable) inmap.get(pair1);
				ii.set(ii.get() + 1);
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Entry<Writable, Writable> e : inmap.entrySet()) {
			context.write((Pair) e.getKey(), (IntWritable) e.getValue());
		}
	}
}
