package striperelative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(StripeMain.class);
		job.setMapperClass(StripeMapper.class);
		job.setReducerClass(StripeReducer.class);
		job.setPartitionerClass(StripeCustomReducer.class);
		job.setMapOutputKeyClass(Text.class);
		//job.setNumReduceTasks(3);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StripeWritable.class);
		FileInputFormat.addInputPath(job,new Path("/user/hadoop/dir/MyTest"));
		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/dir/StripeOutPut"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}