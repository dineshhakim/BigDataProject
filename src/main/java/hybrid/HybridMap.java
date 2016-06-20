package hybrid;

import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.io.IOException;

public class HybridMap extends
		Mapper<LongWritable, Text, HybridPair, IntWritable> {

	private final HybridPair pair = new HybridPair();
	HashMap<String, Integer> map = new HashMap<String, Integer>();

	public void map(LongWritable key, Text line, Context context)
			throws IOException, InterruptedException {
		String text = line.toString();

		String[] numbers = text.split(" ");

		for (int i = 0; i < numbers.length - 1; i++) {
			String num = numbers[i];

			if (num.length() == 0)
				continue;
			for (int j = i + 1; j < numbers.length; j++) {
				if (num.equals(numbers[j]))
					break;
				pair.set(num, numbers[j]);

				if (map.containsKey(pair.toString())) {
					map.put(pair.toString(), map.get(pair.toString()) + 1);
				} else {
					map.put(pair.toString(), 1);
				}
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		HybridPair pair = new HybridPair();
		for (String key : map.keySet()) {
			pair.set(key);
			context.write(pair, new IntWritable(map.get(key)));
		}

	}
}