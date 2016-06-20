
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
 

public class WordCount extends Configured {
	
	public static class MapClass extends Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				word.set(token);
				context.write(word, ONE);
			}
		}
	}
		
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable count = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			count.set(sum);
			context.write(key, count);
		}
	}

	
//	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//
//		Job job = Job.getInstance();
//		//Job job = new Job(getConf());
//		job.setJarByClass(WordCount.class);
//		job.setJobName("wordcount");
//		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		
//        job.setMapperClass(MapClass.class);
//        job.setReducerClass(Reduce.class);
//
//		FileInputFormat.setInputPaths(job, new Path("/user/hadoop/dir/MyTest"));
//		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/dir/MyOut"));
//
//		boolean success = job.waitForCompletion(true);
//		
//		
//	}
	
//	public int run(String[] arg0) throws Exception {		
//		Job job = Job.getInstance();
//		//Job job = new Job(getConf());
//		job.setJarByClass(WordCount.class);
//		job.setJobName("wordcount");
//		
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		
//        job.setMapperClass(MapClass.class);
//        job.setReducerClass(Reduce.class);
//
//		FileInputFormat.setInputPaths(job, new Path("/user/hadoop/dir/MyTest"));
//		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/dir/MyOut"));
//
//		boolean success = job.waitForCompletion(true);
//		return success ? 0 : 1; 
//	}

}