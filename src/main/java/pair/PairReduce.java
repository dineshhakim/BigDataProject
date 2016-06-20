package pair;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class PairReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {		 
		int sum = 0;
		for (IntWritable i : values) {
			sum = sum + i.get();
		}
		context.write(key, new IntWritable(sum));
	}
}