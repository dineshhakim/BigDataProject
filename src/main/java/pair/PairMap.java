package pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PairMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	Map<Text, Integer> map = new HashMap<Text, Integer>();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] allIds = line.split(" ");
		int totalId = allIds.length;
		for (int i = 0; i < totalId; i++) {

			String id = allIds[i];
			for (int j = i + 1; j < totalId; j++) {
				String neighbor = allIds[j];

				if (neighbor.equals(id))
					break;
				Text textPair = new Text(id + "," + neighbor);

				if (map.containsKey(textPair)) {
					int total = map.get(textPair) + 1;
					map.put(textPair, total);
				} else {
					map.put(textPair, 1);
				}
			}

			Iterator<Map.Entry<Text, Integer>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<Text, Integer> entry = it.next();
				Text sKey = entry.getKey();
				int total = entry.getValue().intValue();
				context.write(sKey, new IntWritable(total));
			}
		}
	}
}
