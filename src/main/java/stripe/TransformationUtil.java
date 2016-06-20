package stripe;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

public class TransformationUtil {

	public static MapWritable transform(HashMap<Text, IntWritable> stripeMap) {
		MapWritable mapWritable = new MapWritable();

		Iterator<Text> it = stripeMap.keySet().iterator();
		while (it.hasNext()) {
			Text value = it.next();

			mapWritable.put(value, stripeMap.get(value));
		}

		return mapWritable;
	}

}
