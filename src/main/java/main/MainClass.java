package main;

import hybrid.*;
import hybridrelative.MapperHybrid;
import hybridrelative.Pair;
import hybridrelative.PartitionerHybrid;
import hybridrelative.ReducerHybrid;
import hybridrelative.StripeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import pairrelative.*;
import striperelative.*;

public class MainClass {

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			Job job;
			job = Job.getInstance(conf);
			job.setJarByClass(MainClass.class);
			if (args[2] == "pair") {
				job.setMapperClass(RelativePairFrequencyMap.class);
				job.setReducerClass(PairRelativeReducer.class);
				job.setPartitionerClass(PairCustomPartitioner.class);
				job.setMapOutputKeyClass(PairModel.class);
				job.setMapOutputValueClass(IntWritable.class);
				job.setOutputKeyClass(PairModel.class);
				job.setOutputValueClass(Text.class);

			} else if (args[2] == "stripe") {
				job.setMapperClass(StripeMapper.class);
				job.setReducerClass(StripeReducer.class);
				job.setPartitionerClass(StripeCustomReducer.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(MapWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(StripeWritable.class);

			} else if (args[2] == "hybrid") {
				job.setMapperClass(MapperHybrid.class);
				job.setReducerClass(ReducerHybrid.class);
				job.setPartitionerClass(PartitionerHybrid.class);
				job.setMapOutputKeyClass(Pair.class);
				job.setMapOutputValueClass(IntWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(StripeWritable.class);

			} else if (args[2] == "wordcount") {

			}

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
