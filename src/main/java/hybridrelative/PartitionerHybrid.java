package hybridrelative;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerHybrid extends Partitioner<Pair, IntWritable> {

		@Override
		public int getPartition(Pair key, IntWritable value, int numReducers) {
			// TODO Auto-generated method stub
			char c = key.getPrimary().toString().charAt(0);
			return c % numReducers;
		}

	}