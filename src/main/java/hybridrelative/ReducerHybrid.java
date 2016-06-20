package hybridrelative;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerHybrid extends
			Reducer<Pair, IntWritable, Text, StripeWritable> {
		static Integer total = new Integer(0);
		StripeWritable map = new StripeWritable();
		String lastkey = "";
		public void reduce(Pair key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			if (key.getSecondary().toString().equals("0")) {
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				if (!map.isEmpty()) {
					context.write(new Text(lastkey), map);
				}
				map.clear();
				total = new Integer(sum);
				return;
			}
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			lastkey = key.getPrimary().toString();
			String s = sum + "/" + total.intValue();
			Text t = new Text("");
			t.set(s);
				map.put(new Text(key.getSecondary().toString()), t);
	
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			context.write(new Text(lastkey), map);
		}
	}