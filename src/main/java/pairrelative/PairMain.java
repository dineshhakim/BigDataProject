package pairrelative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(PairMain.class);
		job.setMapperClass(RelativePairFrequencyMap.class);
		job.setReducerClass(PairRelativeReducer.class);
		job.setPartitionerClass(PairCustomPartitioner.class);
	//	job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(PairModel.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(PairModel.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/hadoop/dir/MyTest"));
		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/dir/PairOutPut"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}