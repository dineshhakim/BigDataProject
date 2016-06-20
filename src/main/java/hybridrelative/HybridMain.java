package hybridrelative;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HybridMain {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(HybridMain.class);
		job.setMapperClass(MapperHybrid.class);
		job.setReducerClass(ReducerHybrid.class);
		job.setPartitionerClass(PartitionerHybrid.class);
		//job.setNumReduceTasks(3);
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StripeWritable.class);
		
		
		
		FileInputFormat.addInputPath(job, new Path("/user/hadoop/dir/MyTest"));
		FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/dir/HybridOutPut"));
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}