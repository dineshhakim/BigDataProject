package striperelative;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public  class StripeCustomReducer extends Partitioner<Text, MapWritable> {
	@Override
	public int getPartition(Text key, MapWritable value, int numReducers) {
		char c = key.toString().charAt(0);
		return c % numReducers;
	}

}
