package stripe;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReduce extends Reducer<Text, MapWritable, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		Iterator<MapWritable> iter = values.iterator();

		MapWritable map = new MapWritable();

		System.out.println("I entereed reducer ");
		while (iter.hasNext()) {

			MapWritable innerMap = iter.next();
			Set<Writable> entrySet = innerMap.keySet();
			Iterator<Writable> it = entrySet.iterator();

			while (it.hasNext()) {
					
				Writable innerKey = it.next();
				
				if (map.containsKey(innerKey)) {
					IntWritable neighborVlau = (IntWritable) map.get(innerKey);
					int neighborVaueInteger = neighborVlau.get();
					neighborVaueInteger++;
					map.put(innerKey, new IntWritable(neighborVaueInteger));
				
					if(key.equals(new Text("20")))
					{
					System.out.println("Not first time Reducer"+innerKey+"incremented Value "+neighborVaueInteger);	
					}
				
				} 
				else {
				
					if(key.equals(new Text("20")))
					{
					System.out.println("This is first time Reducer"+innerKey+"incremented Value "+innerMap.get(innerKey));	
					}
					map.put(innerKey, innerMap.get(innerKey));
				}
			}

		}

		StripeMapper stripeMapper=new StripeMapper(map);
		context.write(key,new Text(stripeMapper.toString()));
	}

}
