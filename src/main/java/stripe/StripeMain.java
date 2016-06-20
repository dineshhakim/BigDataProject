package stripe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 

public class StripeMain {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();		
		Job job = Job.getInstance(conf);
		job.setJarByClass(StripeMain.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		job.setMapperClass(StripeMap.class);
		job.setReducerClass(StripeReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		FileInputFormat.addInputPath(job,new Path("/user/hadoop/dir/MyTest"));
		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/dir/StripeOutPut"));

		job.waitForCompletion(true);
	}
}
