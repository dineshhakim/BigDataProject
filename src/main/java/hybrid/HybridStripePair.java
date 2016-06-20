package hybrid;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import stripe.TransformationUtil;

public class HybridStripePair extends Mapper<LongWritable, Text, Text, MapWritable> {

	private HashMap<Text, IntWritable> stripeMap = new HashMap<Text, IntWritable>();
	private Text stripeKey = new Text();

	@Override
	public void map(LongWritable key, Text value, Context output)
			throws IOException, InterruptedException {
		String text = value.toString();

		String[] neighborList = text.split(" ");

		for (int i = 0; i < neighborList.length; i++) {
			String neighbor = neighborList[i];

			stripeMap.clear();

			for (int j = i + 1; j < neighborList.length; j++) {
				if (neighbor.equals(neighborList[j]))
					break;
				if (stripeMap.containsKey(neighborList[j])) {
					IntWritable neighborVlaue = stripeMap.get(neighborList[j]);
					int neighborVaueInteger = neighborVlaue.get();
					neighborVaueInteger++;
					stripeMap.put(new Text(neighborList[j]), new IntWritable(neighborVaueInteger));
			
				} else {
					stripeMap
							.put(new Text(neighborList[j]), new IntWritable(1));
				}
			}

			stripeKey.set(neighbor);
			MapWritable stripeMapWritable = TransformationUtil.transform(stripeMap);
		
			System.out.println("Stripe Map "+stripeMap.size());
			System.out.println("Stripe Writtable Map "+stripeMapWritable.size());
			if(stripeMap.get(20)!=null)
			{
				System.out.println("Stripe Map get 20 "+stripeMap.get(20));
				System.out.println("Stripe Map Writtable get 20 "+stripeMapWritable.get(20));
			}
			
			
			System.out.println("Emit"+stripeKey+"-------------"+stripeMap);
			output.write(stripeKey, stripeMapWritable);
		}
	}

}
