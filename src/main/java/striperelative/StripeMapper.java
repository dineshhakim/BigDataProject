package striperelative;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class StripeMapper extends Mapper<Object, Text, Text, MapWritable> {
	MapWritable inmap = new MapWritable();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		ArrayList<String> list = new ArrayList<String>();
		while (itr.hasMoreTokens()) {
			list.add(itr.nextToken());
		}

		for (int i = 0; i < list.size(); i++) {
			Text t = new Text(list.get(i));
			MapWritable map = new MapWritable();
			for (int j = i + 1; j < list.size(); j++) {
				Text neighbor = new Text(list.get(j));
				if (t.equals(neighbor)) {
					break;
				}
				if (!map.containsKey(neighbor)) {
					map.put(neighbor, new IntWritable(0));
				}
				IntWritable in = (IntWritable) map.get(neighbor);
				in.set(in.get() + 1);
			}
			if (!inmap.containsKey(t)) {
				inmap.put(t, new MapWritable());
			}
			MapWritable m = (MapWritable) inmap.get(t);
			for (Entry<Writable, Writable> e : map.entrySet()) {
				if (!m.containsKey(e.getKey())) {
					m.put(e.getKey(), new IntWritable(0));
				}
				IntWritable in = (IntWritable) m.get(e.getKey());
				in.set(in.get() + ((IntWritable) e.getValue()).get());
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Entry<Writable, Writable> e : inmap.entrySet()) {
			context.write((Text) e.getKey(), (MapWritable) e.getValue());
		}

	}
}