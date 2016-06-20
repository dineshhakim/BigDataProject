package hybrid;

 
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HybridReduce extends Reducer<HybridPair, IntWritable, Text, Text> {
	int total = 0;
	String previous = null;
	// private HashMapStripe map = new HashMapStripe();
	//
	// @Override
	// public void reduce(HybridPair key, Iterable<IntWritable> values,
	// Context context) throws IOException, InterruptedException {
	// String temp = key.getFirst().toString();
	// if (!temp.equals(previous) && previous != null) {
	// map.setFrequency();
	// context.write(new Text(previous), map);
	// map.clear();
	// }
	// Iterator<IntWritable> iter = values.iterator();
	// total = 0;
	// while (iter.hasNext()) {
	// total += iter.next().get();
	// }
	// previous = key.getFirst().toString();
	// map.put(new Text(key.getSecond()), new IntWritable(total));
	// }
	//
	// @Override
	// protected void cleanup(Context context) throws IOException,
	// InterruptedException {
	// map.setFrequency();
	// context.write(new Text(previous), map);
	// super.cleanup(context);
	// }
}
