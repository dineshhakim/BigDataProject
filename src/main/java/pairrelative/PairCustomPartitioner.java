package pairrelative;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairCustomPartitioner extends Partitioner<PairModel, IntWritable> {

	@Override
	public int getPartition(PairModel key, IntWritable value, int numReducers) {
		char c = key.getPrimary().toString().charAt(0);
		return c % numReducers;
	}

}
